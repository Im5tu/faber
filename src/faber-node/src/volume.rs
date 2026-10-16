//! Per-replica volumes under `faber-node`'s volume root.
//!
//! Layout, derived only from (project, env, workload, replica index,
//! volume), every part a DNS label or a number:
//!
//! ```text
//! <root>/<project>/<env>/<workload>/<replica_index>/<volume>/
//!     disk.img        block only: the sparse ext4 image
//!     disk.img.new    block only: while it is being formatted
//!     volume          marker: the kind and uid_base it was created with
//!     mnt/            the mount point handed to containers
//! ```
//!
//! # How a pod gets the volume
//!
//! The volume's root is owned on disk by `uid_base:faber_gid`, mode 0770.
//! The pod's user namespace maps container 0 to `uid_base`, so the
//! container's root is the owner, and faber's group can read and write the
//! contents from the host. containerd bind-mounts `mnt/` as it is: no CRI
//! `uidMappings` and no `mount_setattr` idmap, because the on-disk ids
//! already are the host ids the pod maps. Adding an idmap on top would
//! translate ids 0..65535 and present `uid_base` files as the overflow uid.
//! `nosuid,nodev` is set on the host mount, which a bind keeps and a user
//! namespace cannot clear.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use faber_cri::BoxFuture;
use faber_network_management::CommandRunner;
use faber_node_protocol::{ReplicaRef, VolumeInfo, VolumeKind};

#[derive(Debug, thiserror::Error)]
pub enum VolumeError {
    /// The request cannot be met as asked.
    #[error("{0}")]
    Invalid(String),
    #[error("volume is not mounted: {0}")]
    NotMounted(String),
    #[error("`{cmd}` could not be run: {source}")]
    Spawn {
        cmd: String,
        #[source]
        source: io::Error,
    },
    #[error("`{cmd}` failed ({code}): {stderr}")]
    Command {
        cmd: String,
        code: String,
        stderr: String,
    },
    #[error("{what}: {source}")]
    Io {
        what: String,
        #[source]
        source: io::Error,
    },
}

fn io_err(what: impl Into<String>) -> impl FnOnce(io::Error) -> VolumeError {
    let what = what.into();
    move |source| VolumeError::Io { what, source }
}

/// Identifies a volume. The only inputs of every path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeRef {
    project: String,
    env: String,
    workload: String,
    replica_index: u32,
    volume: String,
}

impl VolumeRef {
    /// Re-checks every name, so a `VolumeRef` always yields plain segments.
    pub fn new(replica: &ReplicaRef, volume: &str) -> Result<Self, VolumeError> {
        replica
            .validate()
            .map_err(|e| VolumeError::Invalid(format!("replica: {e}")))?;
        faber_api_types::name::validate_dns_label(volume)
            .map_err(|e| VolumeError::Invalid(format!("volume_name: {e}")))?;
        Ok(Self {
            project: replica.project.clone(),
            env: replica.env.clone(),
            workload: replica.workload.clone(),
            replica_index: replica.replica_index,
            volume: volume.to_owned(),
        })
    }

    pub fn paths(&self, root: &Path) -> VolumePaths {
        let dir = root
            .join(&self.project)
            .join(&self.env)
            .join(&self.workload)
            .join(self.replica_index.to_string())
            .join(&self.volume);
        VolumePaths {
            image: dir.join("disk.img"),
            staging: dir.join("disk.img.new"),
            mount: dir.join("mnt"),
            marker: dir.join("volume"),
            dir,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumePaths {
    pub dir: PathBuf,
    pub image: PathBuf,
    pub staging: PathBuf,
    pub mount: PathBuf,
    /// Beside the volume, not inside it: records its kind and owner.
    pub marker: PathBuf,
}

#[derive(Debug, Clone)]
pub struct EnsureVolume {
    pub volume: VolumeRef,
    pub kind: VolumeKind,
    pub size_bytes: u64,
    pub uid_base: u32,
}

/// Creates, finds and removes volumes.
pub trait VolumeManager: Send + Sync {
    /// Makes the volume exist and be mounted. Safe to repeat: an existing
    /// block image is never formatted again, and a different size is refused.
    fn ensure<'a>(&'a self, req: &'a EnsureVolume) -> BoxFuture<'a, Result<(), VolumeError>>;

    /// Unmounts the volume and deletes it. Succeeds when it is already gone.
    fn remove<'a>(&'a self, volume: &'a VolumeRef) -> BoxFuture<'a, Result<(), VolumeError>>;

    /// Host directory to hand to a container. The volume must be mounted.
    fn mount_path<'a>(
        &'a self,
        volume: &'a VolumeRef,
    ) -> BoxFuture<'a, Result<PathBuf, VolumeError>>;

    /// Every volume under the root, sorted. Reads the layout, never a path
    /// from a request, and follows no symlink.
    fn list(&self) -> BoxFuture<'_, Result<Vec<VolumeInfo>, VolumeError>>;

    /// Bytes free for unprivileged use on the file system that holds the
    /// root: what the images of block volumes can grow into.
    fn free_bytes(&self) -> BoxFuture<'_, Result<u64, VolumeError>>;

    /// The owner and `uid_base` recorded for every volume, from the markers.
    fn uid_holders(&self) -> BoxFuture<'_, Result<Vec<UidHolder>, VolumeError>>;
}

/// Who a `uid_base` belongs to: a workload. Its revisions and replicas share
/// one base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    pub project: String,
    pub env: String,
    pub workload: String,
}

impl Owner {
    pub fn of(replica: &ReplicaRef) -> Self {
        Self {
            project: replica.project.clone(),
            env: replica.env.clone(),
            workload: replica.workload.clone(),
        }
    }
}

/// One owner's claim on a `uid_base`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UidHolder {
    pub owner: Owner,
    pub uid_base: u32,
}

/// Host mounts. Every one is `nosuid,nodev`.
pub trait Mounter: Send + Sync {
    fn is_mounted<'a>(&'a self, target: &'a Path) -> BoxFuture<'a, Result<bool, VolumeError>>;

    fn mount_block<'a>(
        &'a self,
        image: &'a Path,
        target: &'a Path,
    ) -> BoxFuture<'a, Result<(), VolumeError>>;

    fn mount_ramdisk<'a>(
        &'a self,
        target: &'a Path,
        size_bytes: u64,
        uid: u32,
        gid: u32,
    ) -> BoxFuture<'a, Result<(), VolumeError>>;

    fn resize_ramdisk<'a>(
        &'a self,
        target: &'a Path,
        size_bytes: u64,
    ) -> BoxFuture<'a, Result<(), VolumeError>>;

    fn unmount<'a>(&'a self, target: &'a Path) -> BoxFuture<'a, Result<(), VolumeError>>;
}

/// Runs `prog args`; a failure to start or a non-zero exit is an error.
async fn run(
    runner: &dyn CommandRunner,
    prog: &str,
    args: &[&str],
) -> Result<faber_network_management::CommandOutput, VolumeError> {
    let cmd = || {
        std::iter::once(prog)
            .chain(args.iter().copied())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let out = runner
        .run(prog, args, None)
        .await
        .map_err(|source| VolumeError::Spawn { cmd: cmd(), source })?;
    if out.status_ok {
        Ok(out)
    } else {
        Err(VolumeError::Command {
            cmd: cmd(),
            code: out
                .code
                .map_or_else(|| "killed by signal".to_owned(), |c| format!("exit {c}")),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_owned(),
        })
    }
}

fn path_str(path: &Path) -> Result<&str, VolumeError> {
    path.to_str()
        .ok_or_else(|| VolumeError::Invalid("path is not valid UTF-8".to_owned()))
}

/// Where the host tools live. Programs are always run by absolute path, so
/// nothing in root's `PATH` can stand in for them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolPaths {
    /// `mount`, `umount`, `mountpoint`, `chmod`.
    pub bin_dir: PathBuf,
    /// `mkfs.ext4`.
    pub sbin_dir: PathBuf,
}

impl Default for ToolPaths {
    fn default() -> Self {
        Self {
            bin_dir: PathBuf::from("/usr/bin"),
            sbin_dir: PathBuf::from("/usr/sbin"),
        }
    }
}

impl ToolPaths {
    pub fn bin(&self, name: &str) -> String {
        format!(
            "{}/{name}",
            self.bin_dir.to_string_lossy().trim_end_matches('/')
        )
    }

    pub fn sbin(&self, name: &str) -> String {
        format!(
            "{}/{name}",
            self.sbin_dir.to_string_lossy().trim_end_matches('/')
        )
    }
}

/// [`Mounter`] over `mount`, `umount` and `mountpoint`.
pub struct CommandMounter {
    tools: ToolPaths,
    runner: Arc<dyn CommandRunner>,
}

impl CommandMounter {
    pub fn new(tools: ToolPaths, runner: Arc<dyn CommandRunner>) -> Self {
        Self { tools, runner }
    }
}

impl Mounter for CommandMounter {
    fn is_mounted<'a>(&'a self, target: &'a Path) -> BoxFuture<'a, Result<bool, VolumeError>> {
        Box::pin(async move {
            let target = path_str(target)?;
            let args = ["-q", target];
            let out = self
                .runner
                .run(&self.tools.bin("mountpoint"), &args, None)
                .await
                .map_err(|source| VolumeError::Spawn {
                    cmd: format!("mountpoint -q {target}"),
                    source,
                })?;
            match (out.status_ok, out.code) {
                (true, _) => Ok(true),
                // `mountpoint` exits 32 for a directory that is not one.
                (false, Some(32)) => Ok(false),
                (false, code) => Err(VolumeError::Command {
                    cmd: format!("mountpoint -q {target}"),
                    code: format!("{code:?}"),
                    stderr: String::from_utf8_lossy(&out.stderr).trim().to_owned(),
                }),
            }
        })
    }

    fn mount_block<'a>(
        &'a self,
        image: &'a Path,
        target: &'a Path,
    ) -> BoxFuture<'a, Result<(), VolumeError>> {
        Box::pin(async move {
            let args = [
                "-t",
                "ext4",
                "-o",
                "loop,nosuid,nodev",
                path_str(image)?,
                path_str(target)?,
            ];
            run(&*self.runner, &self.tools.bin("mount"), &args)
                .await
                .map(drop)
        })
    }

    fn mount_ramdisk<'a>(
        &'a self,
        target: &'a Path,
        size_bytes: u64,
        uid: u32,
        gid: u32,
    ) -> BoxFuture<'a, Result<(), VolumeError>> {
        Box::pin(async move {
            let options = format!("size={size_bytes},mode=0770,uid={uid},gid={gid},nosuid,nodev");
            let args = ["-t", "tmpfs", "-o", &options, "tmpfs", path_str(target)?];
            run(&*self.runner, &self.tools.bin("mount"), &args)
                .await
                .map(drop)
        })
    }

    fn resize_ramdisk<'a>(
        &'a self,
        target: &'a Path,
        size_bytes: u64,
    ) -> BoxFuture<'a, Result<(), VolumeError>> {
        Box::pin(async move {
            let options = format!("remount,size={size_bytes},nosuid,nodev");
            let args = ["-o", &options, path_str(target)?];
            run(&*self.runner, &self.tools.bin("mount"), &args)
                .await
                .map(drop)
        })
    }

    fn unmount<'a>(&'a self, target: &'a Path) -> BoxFuture<'a, Result<(), VolumeError>> {
        Box::pin(async move {
            run(
                &*self.runner,
                &self.tools.bin("umount"),
                &[path_str(target)?],
            )
            .await
            .map(drop)
        })
    }
}

/// Runs blocking filesystem work off the async threads.
async fn blocking<T, F>(what: &'static str, f: F) -> Result<T, VolumeError>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result.map_err(io_err(what)),
        Err(join) => Err(VolumeError::Io {
            what: what.to_owned(),
            source: io::Error::other(join),
        }),
    }
}

fn ignore_missing(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// `create_dir_all`, tried again once if a parent vanished meanwhile: a
/// concurrent remove prunes empty parents.
fn create_dirs(path: &Path) -> io::Result<()> {
    create_dirs_with(path, |p| std::fs::create_dir_all(p))
}

fn create_dirs_with(path: &Path, mut make: impl FnMut(&Path) -> io::Result<()>) -> io::Result<()> {
    match make(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => make(path),
        other => other,
    }
}

/// Removes the empty directories above the removed volume `dir`: index,
/// workload, env, project. Stops at the first one that is not empty, and
/// never touches `root` or anything above it. If any level is a symlink,
/// nothing is pruned: the path would lead out of the tree.
fn prune_empty_parents(root: &Path, dir: &Path) -> io::Result<()> {
    let parents: Vec<&Path> = dir
        .ancestors()
        .skip(1)
        .take(4)
        .take_while(|p| *p != root && p.starts_with(root))
        .collect();
    for parent in &parents {
        match std::fs::symlink_metadata(parent) {
            Ok(meta) if meta.file_type().is_symlink() => return Ok(()),
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    for parent in parents {
        match std::fs::remove_dir(parent) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) if e.kind() == io::ErrorKind::DirectoryNotEmpty => break,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// The names in `dir` that are directories of their own, not symlinks, and
/// whose name `accept`s. Sorted. A missing `dir` has none.
fn child_dirs(dir: &Path, accept: impl Fn(&str) -> bool) -> io::Result<Vec<(String, PathBuf)>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut found = Vec::new();
    for entry in entries {
        let entry = entry?;
        // `DirEntry::file_type` describes the entry itself: a symlink to a
        // directory is a symlink here, and is skipped.
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if accept(&name) {
            found.push((name, entry.path()));
        }
    }
    found.sort();
    Ok(found)
}

/// The marker of the volume at `dir`, if it is a plain file with a valid
/// content. A symlink, a damaged marker or none: not a volume of ours.
fn plain_marker(marker: &Path) -> Option<Marker> {
    let meta = std::fs::symlink_metadata(marker).ok()?;
    if !meta.file_type().is_file() {
        return None;
    }
    read_marker(marker).ok().flatten()
}

/// A replica index as `VolumeRef` writes it: `0`, `7`, not `+0`, `00` or `1_0`.
fn is_canonical_index(name: &str) -> bool {
    name.parse::<u32>().is_ok_and(|n| n.to_string() == name)
}

fn is_label(name: &str) -> bool {
    faber_api_types::name::validate_dns_label(name).is_ok()
}

/// Walks `root/<project>/<env>/<workload>/<index>/<volume>/`.
fn scan_volumes(root: &Path) -> io::Result<Vec<VolumeInfo>> {
    Ok(scan_markers(root)?
        .into_iter()
        .map(|(info, _)| info)
        .collect())
}

/// [`scan_volumes`], with each volume's marker.
fn scan_markers(root: &Path) -> io::Result<Vec<(VolumeInfo, Marker)>> {
    let mut out = Vec::new();
    for (project, p) in child_dirs(root, is_label)? {
        for (env, e) in child_dirs(&p, is_label)? {
            for (workload, w) in child_dirs(&e, is_label)? {
                for (index, i) in child_dirs(&w, is_canonical_index)? {
                    for (volume, v) in child_dirs(&i, is_label)? {
                        let Some(marker) = plain_marker(&v.join("volume")) else {
                            continue;
                        };
                        let info = VolumeInfo {
                            project: project.clone(),
                            env: env.clone(),
                            workload: workload.clone(),
                            replica_index: index.parse().unwrap_or_default(),
                            volume,
                            kind: marker.kind,
                        };
                        out.push((info, marker));
                    }
                }
            }
        }
    }
    out.sort_by(|(a, _), (b, _)| {
        (&a.project, &a.env, &a.workload, a.replica_index, &a.volume).cmp(&(
            &b.project,
            &b.env,
            &b.workload,
            b.replica_index,
            &b.volume,
        ))
    });
    Ok(out)
}

/// What a volume was first created as. Kept beside the volume, outside the
/// directory containers can reach, so a later request cannot change a
/// volume's kind or owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Marker {
    kind: VolumeKind,
    uid_base: u32,
}

impl Marker {
    fn render(self) -> String {
        let kind = match self.kind {
            VolumeKind::Block => "block",
            VolumeKind::Ramdisk => "ramdisk",
        };
        format!("kind={kind}\nuid_base={}\n", self.uid_base)
    }

    fn parse(text: &str) -> Option<Self> {
        let mut lines = text.lines();
        let kind = match lines.next()?.strip_prefix("kind=")? {
            "block" => VolumeKind::Block,
            "ramdisk" => VolumeKind::Ramdisk,
            _ => return None,
        };
        let uid_base = lines.next()?.strip_prefix("uid_base=")?.parse().ok()?;
        Some(Self { kind, uid_base })
    }
}

fn read_marker(path: &Path) -> io::Result<Option<Marker>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Marker::parse(&text).map(Some).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the volume marker {} is damaged", path.display()),
            )
        }),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Writes the marker whole or not at all: a temporary file, synced, then
/// renamed over the final name.
fn write_marker(path: &Path, marker: Marker) -> io::Result<()> {
    use std::io::Write;

    let tmp = path.with_extension("tmp");
    ignore_missing(std::fs::remove_file(&tmp))?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)?;
    file.write_all(marker.render().as_bytes())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)
}

/// Removes any half-made image, then creates a sparse root-only file.
fn create_image(staging: &Path, size_bytes: u64) -> io::Result<()> {
    ignore_missing(std::fs::remove_file(staging))?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    // The raw image holds the service's data, so only root may read it,
    // whatever the mode of the directories above.
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let file = options.open(staging)?;
    // Sparse: no blocks are allocated until the filesystem writes them.
    file.set_len(size_bytes)
}

fn image_len(image: &Path) -> io::Result<Option<u64>> {
    match std::fs::metadata(image) {
        Ok(meta) => Ok(Some(meta.len())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// [`VolumeManager`] on the host filesystem.
pub struct HostVolumes {
    root: PathBuf,
    faber_gid: u32,
    tools: ToolPaths,
    runner: Arc<dyn CommandRunner>,
    mounter: Arc<dyn Mounter>,
    /// One lock per volume, so two requests for one volume never race on its
    /// image or its mount.
    locks: std::sync::Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>,
}

impl HostVolumes {
    pub fn new(
        root: PathBuf,
        faber_gid: u32,
        tools: ToolPaths,
        runner: Arc<dyn CommandRunner>,
        mounter: Arc<dyn Mounter>,
    ) -> Self {
        Self {
            root,
            faber_gid,
            tools,
            runner,
            mounter,
            locks: std::sync::Mutex::default(),
        }
    }

    async fn lock(&self, dir: &Path) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut map = self.locks.lock().unwrap_or_else(|e| e.into_inner());
            // A lock that only the map holds is idle: forget it.
            map.retain(|_, l| Arc::strong_count(l) > 1);
            Arc::clone(map.entry(dir.to_path_buf()).or_default())
        };
        lock.lock_owned().await
    }

    /// Forgets the locks nobody holds or waits for.
    fn prune(&self) {
        let mut map = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, l| Arc::strong_count(l) > 1);
    }

    #[cfg(test)]
    fn lock_count(&self) -> usize {
        self.locks.lock().unwrap().len()
    }

    async fn ensure_block(
        &self,
        req: &EnsureVolume,
        paths: &VolumePaths,
    ) -> Result<(), VolumeError> {
        let image = paths.image.clone();
        match blocking("reading the volume image", move || image_len(&image)).await? {
            Some(have) if have != req.size_bytes => {
                return Err(VolumeError::Invalid(format!(
                    "the volume exists with size {have}; resizing is not supported"
                )));
            }
            Some(_) => {}
            None => self.format(req, paths).await?,
        }
        if !self.mounter.is_mounted(&paths.mount).await? {
            self.mounter.mount_block(&paths.image, &paths.mount).await?;
        }
        run(
            &*self.runner,
            &self.tools.bin("chmod"),
            &["0770", path_str(&paths.mount)?],
        )
        .await?;
        Ok(())
    }

    /// Builds the image beside its final name and renames it into place, so
    /// an image that exists is always a formatted one.
    async fn format(&self, req: &EnsureVolume, paths: &VolumePaths) -> Result<(), VolumeError> {
        let (staging, size) = (paths.staging.clone(), req.size_bytes);
        blocking("creating the volume image", move || {
            create_image(&staging, size)
        })
        .await?;
        let owner = format!("root_owner={}:{}", req.uid_base, self.faber_gid);
        let formatted = run(
            &*self.runner,
            &self.tools.sbin("mkfs.ext4"),
            &["-F", "-q", "-E", &owner, path_str(&paths.staging)?],
        )
        .await;
        let (staging, image) = (paths.staging.clone(), paths.image.clone());
        if let Err(e) = formatted {
            let _ = blocking("removing the volume image", move || {
                std::fs::remove_file(&staging)
            })
            .await;
            return Err(e);
        }
        blocking("moving the volume image into place", move || {
            std::fs::rename(&staging, &image)
        })
        .await
    }

    async fn ensure_ramdisk(
        &self,
        req: &EnsureVolume,
        paths: &VolumePaths,
    ) -> Result<(), VolumeError> {
        if self.mounter.is_mounted(&paths.mount).await? {
            self.mounter
                .resize_ramdisk(&paths.mount, req.size_bytes)
                .await
        } else {
            self.mounter
                .mount_ramdisk(&paths.mount, req.size_bytes, req.uid_base, self.faber_gid)
                .await
        }
    }
}

impl VolumeManager for HostVolumes {
    fn ensure<'a>(&'a self, req: &'a EnsureVolume) -> BoxFuture<'a, Result<(), VolumeError>> {
        Box::pin(async move {
            let paths = req.volume.paths(&self.root);
            let _guard = self.lock(&paths.dir).await;
            let (mount, marker_path) = (paths.mount.clone(), paths.marker.clone());
            let (image, staging) = (paths.image.clone(), paths.staging.clone());
            let (existing, has_files) = blocking("creating the volume directory", move || {
                create_dirs(&mount)?;
                Ok((
                    read_marker(&marker_path)?,
                    image.exists() || staging.exists(),
                ))
            })
            .await?;
            let wanted = Marker {
                kind: req.kind,
                uid_base: req.uid_base,
            };
            match existing {
                Some(have) => {
                    if have.kind != wanted.kind {
                        return Err(VolumeError::Invalid(
                            "the volume exists with a different kind".to_owned(),
                        ));
                    }
                    if have.uid_base != wanted.uid_base {
                        return Err(VolumeError::Invalid(
                            "the volume exists with a different uid_base".to_owned(),
                        ));
                    }
                }
                None => {
                    // Something is there that nothing recorded. Its kind and
                    // owner are unknown, so it is not adopted.
                    if has_files || self.mounter.is_mounted(&paths.mount).await? {
                        return Err(VolumeError::Invalid(format!(
                            "the volume at {} has no marker; remove it first",
                            paths.dir.display()
                        )));
                    }
                    // Recorded before anything is made, so no step can leave
                    // a volume without it.
                    let marker_path = paths.marker.clone();
                    blocking("recording the volume", move || {
                        write_marker(&marker_path, wanted)
                    })
                    .await?;
                }
            }
            match req.kind {
                VolumeKind::Block => self.ensure_block(req, &paths).await?,
                VolumeKind::Ramdisk => self.ensure_ramdisk(req, &paths).await?,
            }
            Ok(())
        })
    }

    fn remove<'a>(&'a self, volume: &'a VolumeRef) -> BoxFuture<'a, Result<(), VolumeError>> {
        Box::pin(async move {
            let paths = volume.paths(&self.root);
            let guard = self.lock(&paths.dir).await;
            let result = async {
                let (root, dir) = (self.root.clone(), paths.dir.clone());
                if !blocking("checking the volume", move || Ok(dir.exists())).await? {
                    // Parents an earlier remove left behind go too.
                    let dir = paths.dir.clone();
                    return blocking("removing the empty parents", move || {
                        prune_empty_parents(&root, &dir)
                    })
                    .await;
                }
                if self.mounter.is_mounted(&paths.mount).await? {
                    self.mounter.unmount(&paths.mount).await?;
                    if self.mounter.is_mounted(&paths.mount).await? {
                        return Err(VolumeError::Invalid(
                            "the volume is still mounted after unmounting".to_owned(),
                        ));
                    }
                }
                // Single files and empty directories only: anything under an
                // unexpectedly unmounted `mnt` makes `remove_dir` fail instead
                // of being deleted.
                let marker_tmp = paths.marker.with_extension("tmp");
                let root = self.root.clone();
                blocking("removing the volume", move || {
                    for file in [&paths.image, &paths.staging, &paths.marker, &marker_tmp] {
                        ignore_missing(std::fs::remove_file(file))?;
                    }
                    for dir in [&paths.mount, &paths.dir] {
                        ignore_missing(std::fs::remove_dir(dir))?;
                    }
                    prune_empty_parents(&root, &paths.dir)
                })
                .await
            }
            .await;
            drop(guard);
            self.prune();
            result
        })
    }

    fn mount_path<'a>(
        &'a self,
        volume: &'a VolumeRef,
    ) -> BoxFuture<'a, Result<PathBuf, VolumeError>> {
        Box::pin(async move {
            let paths = volume.paths(&self.root);
            if self.mounter.is_mounted(&paths.mount).await? {
                Ok(paths.mount)
            } else {
                Err(VolumeError::NotMounted(volume.volume.clone()))
            }
        })
    }

    fn list(&self) -> BoxFuture<'_, Result<Vec<VolumeInfo>, VolumeError>> {
        Box::pin(async move {
            let root = self.root.clone();
            blocking("listing the volumes", move || scan_volumes(&root)).await
        })
    }

    fn uid_holders(&self) -> BoxFuture<'_, Result<Vec<UidHolder>, VolumeError>> {
        Box::pin(async move {
            let root = self.root.clone();
            let found = blocking("reading the volume markers", move || scan_markers(&root)).await?;
            Ok(found
                .into_iter()
                .map(|(info, marker)| UidHolder {
                    owner: Owner {
                        project: info.project,
                        env: info.env,
                        workload: info.workload,
                    },
                    uid_base: marker.uid_base,
                })
                .collect())
        })
    }

    fn free_bytes(&self) -> BoxFuture<'_, Result<u64, VolumeError>> {
        Box::pin(async move {
            let root = self.root.clone();
            blocking("measuring the free space of the volume root", move || {
                free_bytes_at(&root)
            })
            .await
        })
    }
}

/// Free space for unprivileged use on the file system of `path`: fragment
/// size times available blocks.
#[cfg(target_os = "linux")]
fn free_bytes_at(path: &Path) -> io::Result<u64> {
    let stat = nix::sys::statvfs::statvfs(path).map_err(io::Error::from)?;
    let bytes = u128::from(stat.fragment_size()) * u128::from(stat.blocks_available());
    Ok(u64::try_from(bytes).unwrap_or(u64::MAX))
}

#[cfg(not(target_os = "linux"))]
fn free_bytes_at(_path: &Path) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "free space is measured on Linux only",
    ))
}

/// In-memory [`VolumeManager`] for tests.
#[doc(hidden)]
pub mod fake {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug, Default)]
    pub struct FakeVolumes {
        /// Ensured volumes: kind, size and owner.
        volumes: Mutex<BTreeMap<String, (VolumeKind, u64, u32)>>,
        root: PathBuf,
        /// What [`VolumeManager::free_bytes`] answers; `None` fails it.
        free_bytes: Mutex<Option<u64>>,
    }

    impl FakeVolumes {
        pub fn new(root: impl Into<PathBuf>) -> Self {
            Self {
                volumes: Mutex::default(),
                root: root.into(),
                free_bytes: Mutex::new(Some(0)),
            }
        }

        /// Sets the free space the fake reports, or makes the measurement
        /// fail.
        pub fn set_free_bytes(&self, free: Option<u64>) {
            *self.free_bytes.lock().unwrap() = free;
        }

        fn key(volume: &VolumeRef) -> String {
            volume
                .paths(Path::new(""))
                .dir
                .to_string_lossy()
                .into_owned()
        }

        pub fn ensured(&self) -> Vec<(String, VolumeKind, u64, u32)> {
            self.volumes
                .lock()
                .unwrap()
                .iter()
                .map(|(k, (kind, size, uid))| (k.clone(), *kind, *size, *uid))
                .collect()
        }
    }

    impl VolumeManager for FakeVolumes {
        fn ensure<'a>(&'a self, req: &'a EnsureVolume) -> BoxFuture<'a, Result<(), VolumeError>> {
            Box::pin(async move {
                // A real ensure takes time (mkfs, mount); yielding lets
                // concurrent callers interleave before the volume exists.
                tokio::task::yield_now().await;
                self.volumes.lock().unwrap().insert(
                    Self::key(&req.volume),
                    (req.kind, req.size_bytes, req.uid_base),
                );
                Ok(())
            })
        }

        fn remove<'a>(&'a self, volume: &'a VolumeRef) -> BoxFuture<'a, Result<(), VolumeError>> {
            Box::pin(async move {
                self.volumes.lock().unwrap().remove(&Self::key(volume));
                Ok(())
            })
        }

        fn list(&self) -> BoxFuture<'_, Result<Vec<VolumeInfo>, VolumeError>> {
            Box::pin(async move {
                let found = self.volumes.lock().unwrap();
                Ok(found
                    .iter()
                    .filter_map(|(dir, (kind, _, _))| {
                        // The key is `<project>/<env>/<workload>/<index>/<volume>`.
                        let mut parts = dir.replace('\\', "/");
                        parts.retain(|c| c != '\0');
                        let parts: Vec<&str> = parts.split('/').filter(|p| !p.is_empty()).collect();
                        let [project, env, workload, index, volume] = parts.as_slice() else {
                            return None;
                        };
                        Some(VolumeInfo {
                            project: (*project).to_owned(),
                            env: (*env).to_owned(),
                            workload: (*workload).to_owned(),
                            replica_index: index.parse().ok()?,
                            volume: (*volume).to_owned(),
                            kind: *kind,
                        })
                    })
                    .collect())
            })
        }

        fn uid_holders(&self) -> BoxFuture<'_, Result<Vec<UidHolder>, VolumeError>> {
            Box::pin(async move {
                let found = self.volumes.lock().unwrap();
                Ok(found
                    .iter()
                    .filter_map(|(dir, (_, _, uid))| {
                        let dir = dir.replace('\\', "/");
                        let mut parts = dir.split('/').filter(|p| !p.is_empty());
                        Some(UidHolder {
                            owner: Owner {
                                project: parts.next()?.to_owned(),
                                env: parts.next()?.to_owned(),
                                workload: parts.next()?.to_owned(),
                            },
                            uid_base: *uid,
                        })
                    })
                    .collect())
            })
        }

        fn free_bytes(&self) -> BoxFuture<'_, Result<u64, VolumeError>> {
            Box::pin(async move {
                (*self.free_bytes.lock().unwrap()).ok_or_else(|| VolumeError::Io {
                    what: "measuring the free space of the volume root".into(),
                    source: io::Error::other("fake failure"),
                })
            })
        }

        fn mount_path<'a>(
            &'a self,
            volume: &'a VolumeRef,
        ) -> BoxFuture<'a, Result<PathBuf, VolumeError>> {
            Box::pin(async move {
                if self
                    .volumes
                    .lock()
                    .unwrap()
                    .contains_key(&Self::key(volume))
                {
                    Ok(volume.paths(&self.root).mount)
                } else {
                    Err(VolumeError::NotMounted(volume.volume.clone()))
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use faber_network_management::CommandOutput;
    use faber_network_management::fake::RecordingRunner;

    use super::*;

    fn replica() -> ReplicaRef {
        ReplicaRef {
            project: "shop".into(),
            env: "prod".into(),
            workload: "redis".into(),
            revision: "a1b2c3d4".into(),
            replica_index: 1,
        }
    }

    struct Harness {
        runner: Arc<RecordingRunner>,
        volumes: HostVolumes,
        root: PathBuf,
        _tmp: tempfile::TempDir,
    }

    fn harness() -> Harness {
        let tmp = tempfile::tempdir().unwrap();
        let runner = Arc::new(RecordingRunner::new());
        // Nothing is mounted until the test says so.
        runner.script("/usr/bin/mountpoint", None, CommandOutput::failure(32, ""));
        let mounter = Arc::new(CommandMounter::new(ToolPaths::default(), runner.clone()));
        let root = tmp.path().to_path_buf();
        Harness {
            volumes: HostVolumes::new(
                root.clone(),
                989,
                ToolPaths::default(),
                runner.clone(),
                mounter,
            ),
            runner,
            root,
            _tmp: tmp,
        }
    }

    fn req(kind: VolumeKind, size_bytes: u64) -> EnsureVolume {
        EnsureVolume {
            volume: VolumeRef::new(&replica(), "data").unwrap(),
            kind,
            size_bytes,
            uid_base: 131072,
        }
    }

    fn lines(h: &Harness) -> Vec<String> {
        let root = h.root.to_string_lossy().into_owned();
        // Every tool the volume code runs is named by absolute path.
        for call in h.runner.calls() {
            assert!(call.prog.starts_with("/usr/"), "{}", call.prog);
        }
        h.runner
            .cmdlines()
            .into_iter()
            .map(|l| l.replace(&root, "<root>").replace('\\', "/"))
            .collect()
    }

    #[test]
    fn paths_derive_only_from_the_five_inputs_and_stay_under_the_root() {
        let root = Path::new("/var/lib/faber-node/volumes");
        let paths = VolumeRef::new(&replica(), "data").unwrap().paths(root);
        assert_eq!(paths.mount, root.join("shop/prod/redis/1/data/mnt"),);
        for p in [&paths.dir, &paths.image, &paths.staging, &paths.mount] {
            assert!(p.starts_with(root));
            assert!(
                p.strip_prefix(root)
                    .unwrap()
                    .components()
                    .all(|c| matches!(c, std::path::Component::Normal(_)))
            );
        }
        // The revision is not part of the path: volumes outlive a redeploy.
        let mut other = replica();
        other.revision = "ffffffff".into();
        assert_eq!(VolumeRef::new(&other, "data").unwrap().paths(root), paths);
    }

    #[test]
    fn traversal_is_refused_before_a_path_exists() {
        for bad in ["..", "../x", "a/b", "", "a.b", "/etc", "A"] {
            assert!(VolumeRef::new(&replica(), bad).is_err(), "{bad:?}");
        }
        let mut r = replica();
        r.project = "..".into();
        assert!(VolumeRef::new(&r, "data").is_err());
        r.project = "shop".into();
        r.workload = "a/b".into();
        assert!(VolumeRef::new(&r, "data").is_err());
    }

    #[tokio::test]
    async fn block_volume_command_sequence() {
        let h = harness();
        h.volumes
            .ensure(&req(VolumeKind::Block, 8 << 20))
            .await
            .unwrap();
        let base = "<root>/shop/prod/redis/1/data";
        assert_eq!(
            lines(&h),
            vec![
                format!("/usr/bin/mountpoint -q {base}/mnt"),
                format!("/usr/sbin/mkfs.ext4 -F -q -E root_owner=131072:989 {base}/disk.img.new"),
                format!("/usr/bin/mountpoint -q {base}/mnt"),
                format!("/usr/bin/mount -t ext4 -o loop,nosuid,nodev {base}/disk.img {base}/mnt"),
                format!("/usr/bin/chmod 0770 {base}/mnt"),
            ]
        );
        // Sparse image of the requested size, in its final place.
        let image = h.root.join("shop/prod/redis/1/data/disk.img");
        assert_eq!(std::fs::metadata(image).unwrap().len(), 8 << 20);
        assert!(!h.root.join("shop/prod/redis/1/data/disk.img.new").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_image_is_readable_by_root_only() {
        use std::os::unix::fs::PermissionsExt;

        let h = harness();
        h.volumes
            .ensure(&req(VolumeKind::Block, 8 << 20))
            .await
            .unwrap();
        let mode = std::fs::metadata(h.root.join("shop/prod/redis/1/data/disk.img"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[tokio::test]
    async fn concurrent_ensures_of_one_new_volume_format_it_once() {
        let h = harness();
        let r = req(VolumeKind::Block, 8 << 20);
        let (a, b, c) = tokio::join!(
            h.volumes.ensure(&r),
            h.volumes.ensure(&r),
            h.volumes.ensure(&r)
        );
        a.unwrap();
        b.unwrap();
        c.unwrap();
        let mkfs = lines(&h)
            .iter()
            .filter(|l| l.starts_with("/usr/sbin/mkfs"))
            .count();
        assert_eq!(mkfs, 1);
    }

    #[tokio::test]
    async fn a_volume_keeps_its_kind_and_owner() {
        let h = harness();
        h.volumes
            .ensure(&req(VolumeKind::Block, 8 << 20))
            .await
            .unwrap();
        let err = h
            .volumes
            .ensure(&req(VolumeKind::Ramdisk, 8 << 20))
            .await
            .unwrap_err();
        assert!(matches!(err, VolumeError::Invalid(m) if m.contains("kind")));
        let mut other_owner = req(VolumeKind::Block, 8 << 20);
        other_owner.uid_base = 196608;
        let err = h.volumes.ensure(&other_owner).await.unwrap_err();
        assert!(matches!(err, VolumeError::Invalid(m) if m.contains("uid_base")));
        // The same request is still fine.
        h.volumes
            .ensure(&req(VolumeKind::Block, 8 << 20))
            .await
            .unwrap();
    }

    fn data_dir(h: &Harness) -> PathBuf {
        h.root.join("shop/prod/redis/1/data")
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_free_space_is_that_of_the_volume_root() {
        let h = harness();
        assert!(h.volumes.free_bytes().await.unwrap() > 0);
        // A root that is not there is an error, not a zero.
        let missing = HostVolumes::new(
            h.root.join("missing"),
            989,
            ToolPaths::default(),
            h.runner.clone(),
            Arc::new(CommandMounter::new(ToolPaths::default(), h.runner.clone())),
        );
        let err = missing.free_bytes().await.unwrap_err();
        assert!(matches!(err, VolumeError::Io { .. }), "{err}");
    }

    #[tokio::test]
    async fn a_volume_with_an_image_but_no_marker_is_refused_not_adopted() {
        // As after a crash between making the image and recording it.
        let h = harness();
        std::fs::create_dir_all(data_dir(&h)).unwrap();
        std::fs::write(data_dir(&h).join("disk.img"), vec![0u8; 8 << 20]).unwrap();
        let err = h
            .volumes
            .ensure(&req(VolumeKind::Block, 8 << 20))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, VolumeError::Invalid(m) if m.contains("no marker")),
            "{err}"
        );
        assert!(!data_dir(&h).join("volume").exists());
        assert!(h.runner.calls().iter().all(|c| !c.prog.contains("mount ")));
    }

    #[tokio::test]
    async fn a_mounted_volume_with_no_marker_is_refused() {
        let h = harness();
        h.runner
            .script("/usr/bin/mountpoint", None, CommandOutput::success());
        let err = h
            .volumes
            .ensure(&req(VolumeKind::Ramdisk, 8 << 20))
            .await
            .unwrap_err();
        assert!(matches!(err, VolumeError::Invalid(m) if m.contains("no marker")));
    }

    #[tokio::test]
    async fn the_marker_is_written_before_anything_is_made() {
        let h = harness();
        h.runner.script(
            "/usr/sbin/mkfs.ext4",
            None,
            CommandOutput::failure(1, "boom"),
        );
        h.volumes
            .ensure(&req(VolumeKind::Block, 8 << 20))
            .await
            .unwrap_err();
        assert!(data_dir(&h).join("volume").exists());
        assert!(!data_dir(&h).join("volume.tmp").exists());
        // So the half-made volume still holds its owner.
        let mut other = req(VolumeKind::Block, 8 << 20);
        other.uid_base = 196608;
        assert!(matches!(
            h.volumes.ensure(&other).await,
            Err(VolumeError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn a_damaged_marker_is_an_error_naming_its_path() {
        let h = harness();
        std::fs::create_dir_all(data_dir(&h)).unwrap();
        std::fs::write(data_dir(&h).join("volume"), "garbage").unwrap();
        let err = h
            .volumes
            .ensure(&req(VolumeKind::Block, 8 << 20))
            .await
            .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("damaged") && text.contains("volume"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_remove_forgets_the_idle_lock_and_the_kind_and_owner() {
        let h = harness();
        let r = req(VolumeKind::Block, 8 << 20);
        h.volumes.ensure(&r).await.unwrap();
        h.volumes.remove(&r.volume).await.unwrap();
        assert_eq!(h.volumes.lock_count(), 0);
        // The kind and owner are forgotten with the volume.
        h.volumes
            .ensure(&req(VolumeKind::Ramdisk, 8 << 20))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn an_existing_image_is_never_formatted_again() {
        let h = harness();
        let r = req(VolumeKind::Block, 8 << 20);
        h.volumes.ensure(&r).await.unwrap();
        h.runner.clear();
        h.volumes.ensure(&r).await.unwrap();
        assert!(
            lines(&h).iter().all(|l| !l.starts_with("/usr/sbin/mkfs")),
            "{:?}",
            lines(&h)
        );
    }

    #[tokio::test]
    async fn an_already_mounted_volume_is_not_mounted_twice() {
        let h = harness();
        let r = req(VolumeKind::Block, 8 << 20);
        h.volumes.ensure(&r).await.unwrap();
        h.runner
            .script("/usr/bin/mountpoint", None, CommandOutput::success());
        h.runner.clear();
        h.volumes.ensure(&r).await.unwrap();
        assert!(lines(&h).iter().all(|l| !l.starts_with("/usr/bin/mount ")));
    }

    #[tokio::test]
    async fn a_size_change_on_an_existing_volume_is_refused() {
        let h = harness();
        h.volumes
            .ensure(&req(VolumeKind::Block, 8 << 20))
            .await
            .unwrap();
        h.runner.clear();
        let err = h
            .volumes
            .ensure(&req(VolumeKind::Block, 16 << 20))
            .await
            .unwrap_err();
        assert!(matches!(err, VolumeError::Invalid(_)));
        assert!(h.runner.calls().is_empty());
    }

    #[tokio::test]
    async fn a_failed_mkfs_leaves_no_image_so_the_next_try_formats_again() {
        let h = harness();
        h.runner.script(
            "/usr/sbin/mkfs.ext4",
            None,
            CommandOutput::failure(1, "boom"),
        );
        let err = h
            .volumes
            .ensure(&req(VolumeKind::Block, 8 << 20))
            .await
            .unwrap_err();
        assert!(matches!(err, VolumeError::Command { .. }));
        assert!(!h.root.join("shop/prod/redis/1/data/disk.img").exists());
        assert!(!h.root.join("shop/prod/redis/1/data/disk.img.new").exists());
    }

    #[tokio::test]
    async fn a_failed_ensure_still_holds_its_uid_base_through_the_marker() {
        let h = harness();
        // The mount fails after the marker is written and the image made.
        h.runner.script(
            "/usr/bin/mount",
            None,
            CommandOutput::failure(32, "no loop device"),
        );
        h.volumes
            .ensure(&req(VolumeKind::Block, 8 << 20))
            .await
            .unwrap_err();
        let holders = h.volumes.uid_holders().await.unwrap();
        assert_eq!(holders.len(), 1);
        assert_eq!(holders[0].uid_base, 131072);
        assert_eq!(holders[0].owner, Owner::of(&replica()));
    }

    #[tokio::test]
    async fn ramdisk_is_tmpfs_with_size_owner_nosuid_nodev() {
        let h = harness();
        h.volumes
            .ensure(&req(VolumeKind::Ramdisk, 64 << 20))
            .await
            .unwrap();
        let base = "<root>/shop/prod/redis/1/data";
        assert_eq!(
            lines(&h),
            vec![
                format!("/usr/bin/mountpoint -q {base}/mnt"),
                format!("/usr/bin/mountpoint -q {base}/mnt"),
                format!(
                    "/usr/bin/mount -t tmpfs -o size=67108864,mode=0770,uid=131072,gid=989,nosuid,nodev tmpfs {base}/mnt"
                ),
            ]
        );
    }

    #[tokio::test]
    async fn every_mount_is_nosuid_nodev() {
        let h = harness();
        h.volumes
            .ensure(&req(VolumeKind::Block, 8 << 20))
            .await
            .unwrap();
        h.volumes
            .ensure(&EnsureVolume {
                volume: VolumeRef::new(&replica(), "cache").unwrap(),
                ..req(VolumeKind::Ramdisk, 8 << 20)
            })
            .await
            .unwrap();
        h.runner
            .script("/usr/bin/mountpoint", None, CommandOutput::success());
        h.volumes
            .ensure(&EnsureVolume {
                volume: VolumeRef::new(&replica(), "cache").unwrap(),
                ..req(VolumeKind::Ramdisk, 16 << 20)
            })
            .await
            .unwrap();
        let mounts: Vec<_> = h
            .runner
            .calls()
            .into_iter()
            .filter(|c| c.prog == "/usr/bin/mount")
            .collect();
        assert_eq!(mounts.len(), 3);
        for call in mounts {
            assert!(
                call.args.iter().any(|a| a.contains("nosuid,nodev")),
                "{}",
                call.cmdline()
            );
        }
    }

    #[tokio::test]
    async fn remove_unmounts_then_deletes_and_is_idempotent() {
        let h = harness();
        let r = req(VolumeKind::Block, 8 << 20);
        h.volumes.ensure(&r).await.unwrap();
        // Mounted before the unmount, gone after it.
        h.runner
            .script("/usr/bin/mountpoint", None, CommandOutput::success());
        h.runner.clear();
        // The `mountpoint` after `umount` must report unmounted: a wrapper
        // swaps the script when `umount` runs.
        struct Flip(Arc<RecordingRunner>);
        impl CommandRunner for Flip {
            fn run<'a>(
                &'a self,
                prog: &'a str,
                args: &'a [&'a str],
                stdin: Option<Vec<u8>>,
            ) -> BoxFuture<'a, io::Result<CommandOutput>> {
                if prog == "/usr/bin/umount" {
                    self.0
                        .script("/usr/bin/mountpoint", None, CommandOutput::failure(32, ""));
                }
                self.0.run(prog, args, stdin)
            }
        }
        let flip = Arc::new(Flip(h.runner.clone()));
        let volumes = HostVolumes::new(
            h.root.clone(),
            989,
            ToolPaths::default(),
            flip.clone(),
            Arc::new(CommandMounter::new(ToolPaths::default(), flip)),
        );
        volumes.remove(&r.volume).await.unwrap();
        let l = lines(&h);
        assert!(l.iter().any(|c| c.starts_with("/usr/bin/umount ")), "{l:?}");
        assert!(!h.root.join("shop/prod/redis/1/data").exists());
        volumes.remove(&r.volume).await.unwrap();
    }

    #[tokio::test]
    async fn remove_refuses_to_delete_a_volume_that_stays_mounted() {
        let h = harness();
        let r = req(VolumeKind::Block, 8 << 20);
        h.volumes.ensure(&r).await.unwrap();
        h.runner
            .script("/usr/bin/mountpoint", None, CommandOutput::success());
        let err = h.volumes.remove(&r.volume).await.unwrap_err();
        assert!(matches!(err, VolumeError::Invalid(_)));
        assert!(h.root.join("shop/prod/redis/1/data/disk.img").exists());
    }

    #[tokio::test]
    async fn mount_path_needs_a_mounted_volume() {
        let h = harness();
        let r = req(VolumeKind::Block, 8 << 20);
        assert!(matches!(
            h.volumes.mount_path(&r.volume).await,
            Err(VolumeError::NotMounted(_))
        ));
        h.runner
            .script("/usr/bin/mountpoint", None, CommandOutput::success());
        assert_eq!(
            h.volumes.mount_path(&r.volume).await.unwrap(),
            h.root.join("shop/prod/redis/1/data/mnt")
        );
    }

    fn write_volume(root: &Path, rel: &str, marker: Option<&str>) {
        let dir = root.join(rel);
        std::fs::create_dir_all(&dir).unwrap();
        if let Some(text) = marker {
            std::fs::write(dir.join("volume"), text).unwrap();
        }
    }

    #[tokio::test]
    async fn list_finds_volumes_by_the_layout_and_their_marker() {
        let h = harness();
        write_volume(
            &h.root,
            "shop/prod/redis/1/data",
            Some("kind=block\nuid_base=65536\n"),
        );
        write_volume(
            &h.root,
            "shop/prod/redis/0/scratch",
            Some("kind=ramdisk\nuid_base=65536\n"),
        );
        write_volume(
            &h.root,
            "blog/dev/web/0/tmp",
            Some("kind=ramdisk\nuid_base=131072\n"),
        );
        let got = h.volumes.list().await.unwrap();
        let names: Vec<_> = got
            .iter()
            .map(|v| {
                (
                    v.project.as_str(),
                    v.workload.as_str(),
                    v.replica_index,
                    v.volume.as_str(),
                    v.kind,
                )
            })
            .collect();
        assert_eq!(
            names,
            [
                ("blog", "web", 0, "tmp", VolumeKind::Ramdisk),
                ("shop", "redis", 0, "scratch", VolumeKind::Ramdisk),
                ("shop", "redis", 1, "data", VolumeKind::Block),
            ]
        );
    }

    #[tokio::test]
    async fn list_skips_what_is_not_a_volume() {
        let h = harness();
        // No marker, a damaged marker, bad names, a stray file.
        write_volume(&h.root, "shop/prod/redis/0/nomarker", None);
        write_volume(&h.root, "shop/prod/redis/0/damaged", Some("rubbish"));
        write_volume(
            &h.root,
            "shop/prod/redis/x/data",
            Some("kind=block\nuid_base=1\n"),
        );
        write_volume(
            &h.root,
            "Shop/prod/redis/0/data",
            Some("kind=block\nuid_base=1\n"),
        );
        write_volume(
            &h.root,
            "shop/prod/redis/0/ok",
            Some("kind=block\nuid_base=1\n"),
        );
        std::fs::write(h.root.join("shop/stray"), "x").unwrap();
        let got = h.volumes.list().await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].volume, "ok");
        // A root that does not exist yet has no volumes.
        let missing = HostVolumes::new(
            h.root.join("absent"),
            989,
            ToolPaths::default(),
            h.runner.clone(),
            Arc::new(CommandMounter::new(ToolPaths::default(), h.runner.clone())),
        );
        assert!(missing.list().await.unwrap().is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn list_never_follows_a_symlink() {
        use std::os::unix::fs::symlink;

        let h = harness();
        write_volume(
            &h.root,
            "shop/prod/redis/0/real",
            Some("kind=block\nuid_base=1\n"),
        );
        // Somewhere else, with a complete volume layout inside.
        let outside = tempfile::tempdir().unwrap();
        write_volume(
            outside.path(),
            "evil/prod/redis/0/data",
            Some("kind=block\nuid_base=1\n"),
        );
        write_volume(outside.path(), "vol", Some("kind=block\nuid_base=1\n"));
        // A symlinked project, a symlinked volume directory, and a volume
        // whose marker is a symlink to a good marker.
        symlink(outside.path().join("evil"), h.root.join("evil")).unwrap();
        symlink(
            outside.path().join("vol"),
            h.root.join("shop/prod/redis/0/linked"),
        )
        .unwrap();
        write_volume(&h.root, "shop/prod/redis/0/markerlink", None);
        symlink(
            outside.path().join("vol/volume"),
            h.root.join("shop/prod/redis/0/markerlink/volume"),
        )
        .unwrap();
        let got = h.volumes.list().await.unwrap();
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].volume, "real");
    }

    #[tokio::test]
    async fn uid_holders_come_from_the_markers() {
        let h = harness();
        write_volume(
            &h.root,
            "shop/prod/redis/0/data",
            Some("kind=block\nuid_base=65536\n"),
        );
        write_volume(
            &h.root,
            "blog/dev/web/2/tmp",
            Some("kind=ramdisk\nuid_base=131072\n"),
        );
        write_volume(&h.root, "blog/dev/web/2/nomarker", None);
        let owner = |p: &str, e: &str, w: &str| Owner {
            project: p.into(),
            env: e.into(),
            workload: w.into(),
        };
        assert_eq!(
            h.volumes.uid_holders().await.unwrap(),
            [
                UidHolder {
                    owner: owner("blog", "dev", "web"),
                    uid_base: 131072
                },
                UidHolder {
                    owner: owner("shop", "prod", "redis"),
                    uid_base: 65536
                },
            ]
        );
    }

    fn remove_ref(rel: &str) -> VolumeRef {
        let mut parts = rel.split('/');
        let mut r = replica();
        r.project = parts.next().unwrap().into();
        r.env = parts.next().unwrap().into();
        r.workload = parts.next().unwrap().into();
        r.replica_index = parts.next().unwrap().parse().unwrap();
        VolumeRef::new(&r, parts.next().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn remove_prunes_empty_parents_up_to_the_root() {
        let h = harness();
        write_volume(
            &h.root,
            "shop/prod/redis/1/data",
            Some("kind=block\nuid_base=1\n"),
        );
        h.volumes
            .remove(&remove_ref("shop/prod/redis/1/data"))
            .await
            .unwrap();
        assert!(h.root.exists());
        assert_eq!(std::fs::read_dir(&h.root).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn remove_keeps_a_parent_that_still_holds_something() {
        let h = harness();
        write_volume(
            &h.root,
            "shop/prod/redis/1/data",
            Some("kind=block\nuid_base=1\n"),
        );
        write_volume(
            &h.root,
            "shop/prod/redis/2/data",
            Some("kind=block\nuid_base=1\n"),
        );
        write_volume(
            &h.root,
            "shop/dev/redis/0/data",
            Some("kind=block\nuid_base=1\n"),
        );
        h.volumes
            .remove(&remove_ref("shop/prod/redis/1/data"))
            .await
            .unwrap();
        assert!(!h.root.join("shop/prod/redis/1").exists());
        assert!(h.root.join("shop/prod/redis/2/data/volume").exists());
        // The sibling env is intact, and so is the project above both.
        assert!(h.root.join("shop/dev/redis/0/data/volume").exists());
    }

    #[tokio::test]
    async fn removing_an_absent_volume_prunes_parents_left_behind() {
        let h = harness();
        std::fs::create_dir_all(h.root.join("shop/prod/redis/1")).unwrap();
        h.volumes
            .remove(&remove_ref("shop/prod/redis/1/data"))
            .await
            .unwrap();
        assert_eq!(std::fs::read_dir(&h.root).unwrap().count(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pruning_never_follows_a_symlink_out_of_the_root() {
        let h = harness();
        write_volume(
            &h.root,
            "shop/prod/redis/1/data",
            Some("kind=block\nuid_base=1\n"),
        );
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(outside.path().join("redis/1")).unwrap();
        // The workload level is a symlink to an outside tree.
        std::fs::remove_dir_all(h.root.join("shop/prod/redis")).unwrap();
        std::os::unix::fs::symlink(outside.path().join("redis"), h.root.join("shop/prod/redis"))
            .unwrap();
        let _ = h
            .volumes
            .remove(&remove_ref("shop/prod/redis/1/data"))
            .await;
        assert!(outside.path().join("redis/1").exists());
    }

    #[test]
    fn prune_stops_at_the_root_whatever_the_depth() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("vol");
        // The volume directory `a/b/c/d/e` is already removed.
        std::fs::create_dir_all(root.join("a/b/c/d")).unwrap();
        prune_empty_parents(&root, &root.join("a/b/c/d/e")).unwrap();
        // Four levels above the volume are pruned; the root itself is not.
        assert!(root.exists());
        assert!(!root.join("a").exists());
        // A path outside the root is left alone.
        std::fs::create_dir_all(tmp.path().join("x/y")).unwrap();
        prune_empty_parents(&root, &tmp.path().join("x/y/z")).unwrap();
        assert!(tmp.path().join("x/y").exists());
    }

    #[test]
    fn a_lost_race_on_mkdir_is_retried_once() {
        let mut calls = 0;
        let ok = create_dirs_with(Path::new("/x"), |_| {
            calls += 1;
            if calls == 1 {
                Err(io::ErrorKind::NotFound.into())
            } else {
                Ok(())
            }
        });
        assert!(ok.is_ok());
        assert_eq!(calls, 2);

        let mut calls = 0;
        let err = create_dirs_with(Path::new("/x"), |_| {
            calls += 1;
            Err(io::ErrorKind::NotFound.into())
        });
        assert_eq!(err.unwrap_err().kind(), io::ErrorKind::NotFound);
        assert_eq!(calls, 2);

        let mut calls = 0;
        let err = create_dirs_with(Path::new("/x"), |_| {
            calls += 1;
            Err(io::ErrorKind::PermissionDenied.into())
        });
        assert_eq!(err.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(calls, 1);
    }
}
