//! `/api/v1/projects/*` and `/api/v1/workloads`, served on the loopback
//! listener only: a spec carries env values, and the peer listener also
//! admits machines that only hold an invite.
//!
//! TODO(security): no authentication. Whoever reaches the loopback API can
//! create and delete projects, deploy any workload and read every env
//! value. Once a runner exists, that is the right to run a container on
//! every node.

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, FromRequestParts, Path, Query, State};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use faber_api_types::workload::{
    CreateProjectRequest, DeployRequest, DeployResponse, ProjectListResponse, ProjectResponse,
    WorkloadListResponse, WorkloadQuery, WorkloadResponse, limits,
};
use serde::de::DeserializeOwned;

use crate::error::ApiJson;
use crate::service::{FaberService, ServiceError, ServiceResult};

/// The project and workload routes, relative to `/api/v1`. A request body
/// over [`limits::MAX_HTTP_BODY_BYTES`] is refused with 400 `bad_request`.
pub fn routes() -> Router<FaberService> {
    let environment = "/projects/{project}/environments/{environment}";
    Router::new()
        .route("/projects", post(create_project).get(list_projects))
        .route("/projects/{project}", get(project).delete(delete_project))
        .route(environment, put(add_environment).delete(remove_environment))
        .route(&format!("{environment}/deploy"), post(deploy))
        .route(
            &format!("{environment}/workloads/{{workload}}"),
            get(workload).delete(remove_workload),
        )
        .route("/workloads", get(list_workloads))
        .layer(DefaultBodyLimit::max(limits::MAX_HTTP_BODY_BYTES))
}

/// `Path` extractor whose rejections are [`ErrorBody`] 400s, like
/// [`ApiJson`]. The names it yields are not checked here; the service
/// checks every one of them.
///
/// [`ErrorBody`]: faber_api_types::ErrorBody
struct Names<T>(T);

impl<S, T> FromRequestParts<S> for Names<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ServiceError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match Path::<T>::from_request_parts(parts, state).await {
            Ok(Path(names)) => Ok(Names(names)),
            Err(e) => Err(ServiceError::BadRequest(PathRejection::body_text(&e))),
        }
    }
}

/// `Query` extractor whose rejections are [`ErrorBody`] 400s.
///
/// [`ErrorBody`]: faber_api_types::ErrorBody
struct ApiQuery<T>(T);

impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ServiceError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(query)) => Ok(ApiQuery(query)),
            Err(e) => Err(ServiceError::BadRequest(QueryRejection::body_text(&e))),
        }
    }
}

/// 201 with the new project. 409 `project_exists`.
async fn create_project(
    State(svc): State<FaberService>,
    ApiJson(req): ApiJson<CreateProjectRequest>,
) -> ServiceResult<(StatusCode, Json<ProjectResponse>)> {
    Ok((StatusCode::CREATED, Json(svc.create_project(req).await?)))
}

/// 200 with every project as this node has it.
async fn list_projects(
    State(svc): State<FaberService>,
) -> ServiceResult<Json<ProjectListResponse>> {
    Ok(Json(svc.list_projects().await?))
}

/// 200 with the project as this node has it.
async fn project(
    State(svc): State<FaberService>,
    Names(project): Names<String>,
) -> ServiceResult<Json<ProjectResponse>> {
    Ok(Json(svc.project(&project).await?))
}

/// 204 once the project is gone. 409 `not_empty` while it has a workload.
async fn delete_project(
    State(svc): State<FaberService>,
    Names(project): Names<String>,
) -> ServiceResult<StatusCode> {
    svc.delete_project(&project).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// 200 with the project and its new environment. 409 `project_exists` when
/// the project has the environment already. The request has no body.
async fn add_environment(
    State(svc): State<FaberService>,
    Names((project, environment)): Names<(String, String)>,
) -> ServiceResult<Json<ProjectResponse>> {
    Ok(Json(svc.add_environment(&project, &environment).await?))
}

/// 204 once the environment is gone. 409 `not_empty` while it has a
/// workload.
async fn remove_environment(
    State(svc): State<FaberService>,
    Names((project, environment)): Names<(String, String)>,
) -> ServiceResult<StatusCode> {
    svc.remove_environment(&project, &environment).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// 200 with what the deploy did. 400 `bad_request` lists the rules the
/// workloads break. The request carries env values; it must never be
/// logged.
async fn deploy(
    State(svc): State<FaberService>,
    Names((project, environment)): Names<(String, String)>,
    ApiJson(req): ApiJson<DeployRequest>,
) -> ServiceResult<Json<DeployResponse>> {
    Ok(Json(svc.deploy(&project, &environment, req).await?))
}

/// 200 with the workloads the query matches, as this node has them.
async fn list_workloads(
    State(svc): State<FaberService>,
    ApiQuery(query): ApiQuery<WorkloadQuery>,
) -> ServiceResult<Json<WorkloadListResponse>> {
    Ok(Json(svc.list_workloads(query).await?))
}

/// 200 with the workload and its spec. The response carries env values; it
/// must never be logged.
async fn workload(
    State(svc): State<FaberService>,
    Names((project, environment, workload)): Names<(String, String, String)>,
) -> ServiceResult<Json<WorkloadResponse>> {
    let found = svc.workload(&project, &environment, &workload).await?;
    Ok(Json(found))
}

/// 204 once the workload is gone.
async fn remove_workload(
    State(svc): State<FaberService>,
    Names((project, environment, workload)): Names<(String, String, String)>,
) -> ServiceResult<StatusCode> {
    svc.remove_workload(&project, &environment, &workload)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Method, Request, header};
    use faber_api_types::workload::{Exposure, Scale};
    use faber_api_types::{ErrorBody, FORWARDED_HEADER, error_code};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;
    use crate::api::tests::{local, peer, send};
    use crate::dto::PeerWorkloadResponse;
    use crate::testing::TestNode;

    const SHOP: &str = "/api/v1/projects/shop";
    const STAGING: &str = "/api/v1/projects/shop/environments/staging";

    fn request(method: Method, host: &str, uri: &str, body: Option<String>) -> Request<Body> {
        let rb = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, host);
        match body {
            Some(json) => rb
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json)),
            None => rb.body(Body::empty()),
        }
        .unwrap()
    }

    /// A request to the loopback listener.
    fn local_request(method: Method, uri: &str, body: Option<&Value>) -> Request<Body> {
        request(method, "127.0.0.1:7400", uri, body.map(Value::to_string))
    }

    async fn call<T: DeserializeOwned>(
        node: &TestNode,
        method: Method,
        uri: &str,
        body: Option<&Value>,
    ) -> (u16, T) {
        send(local(node), local_request(method, uri, body)).await
    }

    /// The status and the code of a request that must be refused.
    async fn refused(
        node: &TestNode,
        method: Method,
        uri: &str,
        body: Option<&Value>,
    ) -> (u16, String) {
        let (status, err): (_, ErrorBody) = call(node, method, uri, body).await;
        (status, err.error)
    }

    /// A request that must be answered 204 without a body.
    #[track_caller]
    fn no_content(node: &TestNode, uri: &str) -> impl Future<Output = ()> {
        let app = local(node);
        let req = local_request(Method::DELETE, uri, None);
        let caller = std::panic::Location::caller();
        async move {
            let resp = app.oneshot(req).await.unwrap();
            let status = resp.status().as_u16();
            let bytes = resp.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(
                (status, bytes.as_ref()),
                (204, b"".as_slice()),
                "{caller}: {}",
                String::from_utf8_lossy(&bytes)
            );
        }
    }

    fn spec(image: &str) -> Value {
        json!({"containers": [{"name": "main", "image": image}]})
    }

    async fn create_shop(node: &TestNode) -> ProjectResponse {
        let body = json!({"name": "shop"});
        let (status, shop) = call(node, Method::POST, "/api/v1/projects", Some(&body)).await;
        assert_eq!(status, 201);
        shop
    }

    async fn deploy_to(node: &TestNode, environment: &str, body: Value) -> DeployResponse {
        let uri = format!("{environment}/deploy");
        let (status, done) = call(node, Method::POST, &uri, Some(&body)).await;
        assert_eq!(status, 200);
        done
    }

    /// The workloads `query` matches, as `project/environment/name`.
    async fn listed(node: &TestNode, query: &str) -> Vec<String> {
        let uri = format!("/api/v1/workloads{query}");
        let (status, found): (_, WorkloadListResponse) = call(node, Method::GET, &uri, None).await;
        assert_eq!(status, 200, "{query}");
        found
            .workloads
            .into_iter()
            .map(|w| format!("{}/{}/{}", w.project, w.environment, w.name))
            .collect()
    }

    /// One request per public route, each valid once project `shop` has a
    /// workload `web` in `staging`.
    fn every_route() -> Vec<(Method, String, Option<Value>)> {
        let web = format!("{STAGING}/workloads/web");
        vec![
            (
                Method::POST,
                "/api/v1/projects".into(),
                Some(json!({"name": "blog"})),
            ),
            (Method::GET, "/api/v1/projects".into(), None),
            (Method::GET, SHOP.into(), None),
            (Method::PUT, format!("{SHOP}/environments/dev"), None),
            (
                Method::POST,
                format!("{STAGING}/deploy"),
                Some(json!({"workloads": {"web": spec("nginx")}})),
            ),
            (Method::GET, "/api/v1/workloads".into(), None),
            (Method::GET, web.clone(), None),
            (Method::DELETE, web, None),
            (Method::DELETE, format!("{SHOP}/environments/dev"), None),
            (Method::DELETE, SHOP.into(), None),
        ]
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_project_routes_create_read_and_delete() {
        let node = TestNode::start_initialized().await;
        let (status, none): (_, ProjectListResponse) =
            call(&node, Method::GET, "/api/v1/projects", None).await;
        assert_eq!((status, none.projects.len()), (200, 0));

        // Without `environments` a project gets the two defaults.
        let shop = create_shop(&node).await;
        assert_eq!(shop.name, "shop");
        let names: Vec<&str> = shop.environments.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["production", "staging"]);
        assert!(shop.environments.iter().all(|e| e.workloads == 0));

        let body = json!({"name": "blog", "environments": ["dev"]});
        let (status, blog): (_, ProjectResponse) =
            call(&node, Method::POST, "/api/v1/projects", Some(&body)).await;
        assert_eq!(status, 201);
        assert_eq!(blog.environments.len(), 1);

        let (status, all): (_, ProjectListResponse) =
            call(&node, Method::GET, "/api/v1/projects", None).await;
        assert_eq!(status, 200);
        assert_eq!(all.projects, vec![blog.clone(), shop.clone()]);
        let (status, got): (_, ProjectResponse) = call(&node, Method::GET, SHOP, None).await;
        assert_eq!((status, got), (200, shop));

        // An environment is added without a body, and with one.
        let dev = format!("{SHOP}/environments/dev");
        let (status, got): (_, ProjectResponse) = call(&node, Method::PUT, &dev, None).await;
        assert_eq!(status, 200);
        let names: Vec<&str> = got.environments.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["dev", "production", "staging"]);
        let qa = format!("{SHOP}/environments/qa");
        let (status, got): (_, ProjectResponse) =
            call(&node, Method::PUT, &qa, Some(&json!({}))).await;
        assert_eq!((status, got.environments.len()), (200, 4));

        no_content(&node, &dev).await;
        let (status, got): (_, ProjectResponse) = call(&node, Method::GET, SHOP, None).await;
        assert_eq!((status, got.environments.len()), (200, 3));
        no_content(&node, "/api/v1/projects/blog").await;
        let (status, all): (_, ProjectListResponse) =
            call(&node, Method::GET, "/api/v1/projects", None).await;
        assert_eq!((status, all.projects.len()), (200, 1));
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_workload_routes_deploy_read_and_delete() {
        let node = TestNode::start_initialized().await;
        create_shop(&node).await;
        let both = json!({"workloads": {
            "web": spec("nginx"),
            "api": {
                "scale": {"replicas": 3},
                "expose": "cluster",
                "containers": [{"name": "main", "image": "api:1", "env": {"TOKEN": "s3cret"}}],
            },
        }});

        // `prune` is optional and off.
        let first = deploy_to(&node, STAGING, both.clone()).await;
        assert_eq!(first.created, vec!["api", "web"]);
        assert!(first.generation > 0);
        let again = deploy_to(&node, STAGING, both).await;
        assert_eq!(
            again,
            DeployResponse {
                generation: first.generation,
                unchanged: vec!["api".into(), "web".into()],
                ..DeployResponse::default()
            }
        );

        let (status, list): (_, WorkloadListResponse) =
            call(&node, Method::GET, "/api/v1/workloads", None).await;
        assert_eq!(status, 200);
        let api = &list.workloads[0];
        assert_eq!(api.fqdn, "api.staging.shop.faber.internal");
        assert_eq!(api.scale, Scale::Replicas(3));
        assert_eq!(api.expose, Exposure::Cluster);
        assert_eq!(api.revision, first.generation);
        // A summary carries no part of the spec that can hold a secret.
        let (_, raw): (_, Value) = call(&node, Method::GET, "/api/v1/workloads", None).await;
        assert!(!raw.to_string().contains("s3cret"), "{raw}");

        let uri = format!("{STAGING}/workloads/api");
        let (status, api): (_, WorkloadResponse) = call(&node, Method::GET, &uri, None).await;
        assert_eq!(status, 200);
        assert_eq!(
            (api.project.as_str(), api.environment.as_str()),
            ("shop", "staging")
        );
        assert_eq!(api.name, "api");
        assert_eq!(api.fqdn, "api.staging.shop.faber.internal");
        assert_eq!(
            (api.revision, api.generation),
            (first.generation, first.generation)
        );
        assert_eq!(api.spec.containers[0].env["TOKEN"], "s3cret");

        // With `prune` the deploy removes what it does not name.
        let pruned = deploy_to(
            &node,
            STAGING,
            json!({"workloads": {"web": spec("nginx:2")}, "prune": true}),
        )
        .await;
        assert_eq!(pruned.updated, vec!["web"]);
        assert_eq!(pruned.removed, vec!["api"]);
        assert!(pruned.generation > first.generation);
        assert_eq!(listed(&node, "").await, vec!["shop/staging/web"]);
        assert_eq!(
            refused(&node, Method::GET, &uri, None).await,
            (404, error_code::NOT_FOUND.into())
        );

        let web = format!("{STAGING}/workloads/web");
        no_content(&node, &web).await;
        assert_eq!(listed(&node, "").await, Vec::<String>::new());
        for method in [Method::GET, Method::DELETE] {
            assert_eq!(
                refused(&node, method, &web, None).await,
                (404, error_code::NOT_FOUND.into())
            );
        }
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_workload_list_takes_filters() {
        let node = TestNode::start_initialized().await;
        create_shop(&node).await;
        let blog = json!({"name": "blog", "environments": ["staging", "dev"]});
        let (status, _): (_, ProjectResponse) =
            call(&node, Method::POST, "/api/v1/projects", Some(&blog)).await;
        assert_eq!(status, 201);
        let web = || json!({"workloads": {"web": spec("nginx")}});
        deploy_to(&node, STAGING, web()).await;
        deploy_to(&node, &format!("{SHOP}/environments/production"), web()).await;
        let site = json!({"workloads": {"site": spec("ghost")}});
        deploy_to(&node, "/api/v1/projects/blog/environments/staging", site).await;

        assert_eq!(
            listed(&node, "").await,
            vec![
                "blog/staging/site",
                "shop/production/web",
                "shop/staging/web"
            ]
        );
        assert_eq!(
            listed(&node, "?project=shop").await,
            vec!["shop/production/web", "shop/staging/web"]
        );
        assert_eq!(
            listed(&node, "?environment=staging").await,
            vec!["blog/staging/site", "shop/staging/web"]
        );
        assert_eq!(
            listed(&node, "?project=shop&environment=production").await,
            vec!["shop/production/web"]
        );
        // A filter for something the cluster does not have matches
        // nothing; that is no error.
        for query in [
            "?project=wiki",
            "?environment=qa",
            "?project=blog&environment=dev",
            "?project=blog&environment=production",
        ] {
            assert_eq!(listed(&node, query).await, Vec::<String>::new(), "{query}");
        }
        // A filter that is no name is one.
        for query in [
            "?project=Shop",
            "?environment=a_b",
            "?project=",
            "?project=shop&environment=..%2Fx",
        ] {
            let uri = format!("/api/v1/workloads{query}");
            assert_eq!(
                refused(&node, Method::GET, &uri, None).await,
                (400, error_code::BAD_REQUEST.into()),
                "{query}"
            );
        }
        node.shutdown().await;
    }

    /// A spec carries env values, and the peer listener admits machines
    /// that only hold an invite.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_peer_listener_serves_none_of_the_routes() {
        let node = TestNode::start_initialized().await;
        create_shop(&node).await;
        deploy_to(&node, STAGING, json!({"workloads": {"web": spec("nginx")}})).await;
        let before = node.service.list_projects().await.unwrap();

        let host = node.peer_addr.to_string();
        let routes = every_route();
        assert_eq!(routes.len(), 10);
        for (method, uri, body) in &routes {
            let req = request(
                method.clone(),
                &host,
                uri,
                body.as_ref().map(Value::to_string),
            );
            let (status, err): (_, ErrorBody) = send(peer(&node), req).await;
            assert_eq!(
                (status, err.error.as_str()),
                (404, error_code::NOT_FOUND),
                "{method} {uri}"
            );
        }
        assert_eq!(node.service.list_projects().await.unwrap(), before);
        assert_eq!(listed(&node, "").await, vec!["shop/staging/web"]);

        // The same requests are served on the loopback listener.
        for (method, uri, body) in routes {
            let req = local_request(method.clone(), &uri, body.as_ref());
            let resp = local(&node).oneshot(req).await.unwrap();
            assert!(resp.status().is_success(), "{method} {uri}");
        }
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_routes_are_guarded_like_the_other_loopback_routes() {
        let node = TestNode::start_initialized().await;
        create_shop(&node).await;
        deploy_to(&node, STAGING, json!({"workloads": {"web": spec("nginx")}})).await;
        let before = node.service.list_projects().await.unwrap();

        for (method, uri, body) in every_route() {
            let body = body.as_ref().map(Value::to_string);
            // A Host that is not loopback: a rebound DNS name.
            let req = request(method.clone(), "evil.example:7400", &uri, body.clone());
            let (status, err): (_, ErrorBody) = send(local(&node), req).await;
            assert_eq!(
                (status, err.error.as_str()),
                (403, error_code::FORBIDDEN),
                "{method} {uri}"
            );

            // Any Origin: a browser page.
            let mut req = request(method.clone(), "127.0.0.1:7400", &uri, body);
            req.headers_mut()
                .insert(header::ORIGIN, "http://127.0.0.1:7400".parse().unwrap());
            let (status, err): (_, ErrorBody) = send(local(&node), req).await;
            assert_eq!(
                (status, err.error.as_str()),
                (403, error_code::FORBIDDEN),
                "{method} {uri}"
            );
        }
        // None of them changed anything.
        assert_eq!(node.service.list_projects().await.unwrap(), before);
        assert_eq!(listed(&node, "").await, vec!["shop/staging/web"]);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_bad_name_is_a_bad_request_wherever_it_is() {
        let node = TestNode::start_initialized().await;
        create_shop(&node).await;
        deploy_to(&node, STAGING, json!({"workloads": {"web": spec("nginx")}})).await;
        let web = json!({"workloads": {"web": spec("nginx")}});
        let p = "/api/v1/projects";

        // `a%2Fb` reaches the handler as `a/b`.
        for bad in ["Shop", "a_b", "a.b", "a%2Fb", "-a", "..", "%2E%2E%2Fshop"] {
            let cases = [
                (Method::GET, format!("{p}/{bad}"), None),
                (Method::DELETE, format!("{p}/{bad}"), None),
                (Method::PUT, format!("{p}/{bad}/environments/dev"), None),
                (Method::PUT, format!("{SHOP}/environments/{bad}"), None),
                (
                    Method::DELETE,
                    format!("{p}/{bad}/environments/staging"),
                    None,
                ),
                (Method::DELETE, format!("{SHOP}/environments/{bad}"), None),
                (
                    Method::POST,
                    format!("{p}/{bad}/environments/staging/deploy"),
                    Some(&web),
                ),
                (
                    Method::POST,
                    format!("{SHOP}/environments/{bad}/deploy"),
                    Some(&web),
                ),
                (
                    Method::GET,
                    format!("{p}/{bad}/environments/staging/workloads/web"),
                    None,
                ),
                (
                    Method::GET,
                    format!("{SHOP}/environments/{bad}/workloads/web"),
                    None,
                ),
                (Method::GET, format!("{STAGING}/workloads/{bad}"), None),
                (
                    Method::DELETE,
                    format!("{p}/{bad}/environments/staging/workloads/web"),
                    None,
                ),
                (
                    Method::DELETE,
                    format!("{SHOP}/environments/{bad}/workloads/web"),
                    None,
                ),
                (Method::DELETE, format!("{STAGING}/workloads/{bad}"), None),
            ];
            for (method, uri, body) in cases {
                assert_eq!(
                    refused(&node, method.clone(), &uri, body).await,
                    (400, error_code::BAD_REQUEST.into()),
                    "{method} {uri}"
                );
            }
        }

        // Names in a body.
        for bad in ["Blog", "a_b", "a/b", "", "../shop"] {
            let project = json!({"name": bad});
            let environment = json!({"name": "blog", "environments": ["dev", bad]});
            for body in [project, environment] {
                assert_eq!(
                    refused(&node, Method::POST, p, Some(&body)).await,
                    (400, error_code::BAD_REQUEST.into()),
                    "{body}"
                );
            }
            let body = json!({"workloads": {bad: spec("nginx")}});
            let uri = format!("{STAGING}/deploy");
            assert_eq!(
                refused(&node, Method::POST, &uri, Some(&body)).await,
                (400, error_code::BAD_REQUEST.into()),
                "{body}"
            );
        }
        // An empty list of environments is not the default.
        let none = json!({"name": "blog", "environments": []});
        assert_eq!(
            refused(&node, Method::POST, p, Some(&none)).await,
            (400, error_code::BAD_REQUEST.into())
        );

        // Nothing was written.
        let projects = node.service.list_projects().await.unwrap().projects;
        assert_eq!(projects.len(), 1);
        assert_eq!(listed(&node, "").await, vec!["shop/staging/web"]);
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deploy_the_model_refuses_is_a_bad_request() {
        let node = TestNode::start_initialized().await;
        create_shop(&node).await;
        let uri = format!("{STAGING}/deploy");
        let deploy = |body: Value| {
            let node = &node;
            let uri = &uri;
            async move {
                let (status, err): (_, ErrorBody) =
                    call(node, Method::POST, uri, Some(&body)).await;
                assert_eq!(
                    (status, err.error.as_str()),
                    (400, error_code::BAD_REQUEST),
                    "{body}"
                );
                err.message.unwrap_or_default()
            }
        };

        // One rule broken, next to a valid workload.
        let message = deploy(json!({"workloads": {
            "web": spec("nginx"),
            "api": {"containers": []},
        }}))
        .await;
        assert!(message.contains("api.containers: "), "{message}");
        assert!(!message.contains(" more"), "{message}");

        // Many: the first 20 are listed and the rest counted.
        let many: serde_json::Map<String, Value> = (10..40)
            .map(|n| (format!("w{n}"), json!({"containers": []})))
            .collect();
        let message = deploy(json!({"workloads": many, "prune": true})).await;
        assert!(message.contains("w10.containers: "), "{message}");
        assert!(message.contains("w29.containers: "), "{message}");
        assert!(!message.contains("w30.containers"), "{message}");
        assert!(message.ends_with("; and 10 more"), "{message}");

        // A field the model does not have, at each level of a spec.
        let unknown = [
            json!({"privileged": true, "containers": [{"name": "main", "image": "nginx"}]}),
            json!({"containers": [{"name": "main", "image": "nginx", "privileged": true}]}),
            json!({"containers": [{"name": "main", "image": "nginx",
                                   "resources": {"limits": {"gpus": 1}}}]}),
            json!({"volumes": {"data": {"type": "host_path", "path": "/data", "mode": 511}},
                   "containers": [{"name": "main", "image": "nginx"}]}),
        ];
        for spec in unknown {
            let message = deploy(json!({"workloads": {"web": spec}})).await;
            assert!(message.contains("unknown field"), "{message}");
        }
        // And what is no deploy at all.
        for body in [json!({}), json!({"workloads": []}), json!({"prune": true})] {
            deploy(body).await;
        }
        let req = request(
            Method::POST,
            "127.0.0.1:7400",
            &uri,
            Some("{not json".into()),
        );
        let (status, err): (_, ErrorBody) = send(local(&node), req).await;
        assert_eq!((status, err.error.as_str()), (400, error_code::BAD_REQUEST));

        assert_eq!(listed(&node, "").await, Vec::<String>::new());
        node.shutdown().await;
    }

    /// A body over 1 MiB is refused before it is parsed, on the public
    /// routes and on the peer route.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_oversized_body_is_refused() {
        let node = TestNode::start_initialized().await;
        create_shop(&node).await;
        // Valid JSON of a valid deploy, were it not for its size.
        let pad = "x".repeat(limits::MAX_HTTP_BODY_BYTES);
        let deploy = json!({"workloads": {"web": {"containers": [
            {"name": "main", "image": "nginx", "env": {"PAD": pad}},
        ]}}});
        let uri = format!("{STAGING}/deploy");
        let (status, err): (_, ErrorBody) = call(&node, Method::POST, &uri, Some(&deploy)).await;
        assert_eq!((status, err.error.as_str()), (400, error_code::BAD_REQUEST));
        let message = err.message.unwrap_or_default();
        assert!(message.contains("length limit exceeded"), "{message}");

        let create = json!({"name": "blog", "environments": [pad]});
        let (status, err): (_, ErrorBody) =
            call(&node, Method::POST, "/api/v1/projects", Some(&create)).await;
        assert_eq!((status, err.error.as_str()), (400, error_code::BAD_REQUEST));
        let message = err.message.unwrap_or_default();
        assert!(message.contains("length limit exceeded"), "{message}");

        let mut forwarded = deploy;
        forwarded["op"] = "apply_workloads".into();
        forwarded["project"] = "shop".into();
        forwarded["environment"] = "staging".into();
        forwarded["prune"] = false.into();
        let host = node.peer_addr.to_string();
        let req = request(
            Method::POST,
            &host,
            "/raft/cluster/workloads",
            Some(forwarded.to_string()),
        );
        let (status, err): (_, ErrorBody) = send(peer(&node), req).await;
        assert_eq!((status, err.error.as_str()), (400, error_code::BAD_REQUEST));
        let message = err.message.unwrap_or_default();
        assert!(message.contains("length limit exceeded"), "{message}");

        // A body under the HTTP limit and over the deploy limit gets as
        // far as the validation.
        let pad = "x".repeat(limits::MAX_ENV_VALUE_BYTES);
        let env: serde_json::Map<String, Value> =
            (0..5).map(|n| (format!("PAD_{n}"), json!(pad))).collect();
        let large = json!({"workloads": {"web": {"containers": [
            {"name": "main", "image": "nginx", "env": env},
        ]}}});
        let (status, err): (_, ErrorBody) = call(&node, Method::POST, &uri, Some(&large)).await;
        assert_eq!((status, err.error.as_str()), (400, error_code::BAD_REQUEST));
        let message = err.message.unwrap_or_default();
        assert!(message.starts_with("invalid workloads: "), "{message}");

        assert_eq!(listed(&node, "").await, Vec::<String>::new());
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn what_does_not_exist_is_not_found() {
        let node = TestNode::start_initialized().await;
        create_shop(&node).await;
        let web = json!({"workloads": {"web": spec("nginx")}});
        let blog = "/api/v1/projects/blog";
        let cases = [
            (Method::GET, blog.to_string(), None),
            (Method::DELETE, blog.to_string(), None),
            (Method::PUT, format!("{blog}/environments/dev"), None),
            (Method::DELETE, format!("{blog}/environments/dev"), None),
            (Method::DELETE, format!("{SHOP}/environments/dev"), None),
            (
                Method::POST,
                format!("{blog}/environments/staging/deploy"),
                Some(&web),
            ),
            (
                Method::POST,
                format!("{SHOP}/environments/dev/deploy"),
                Some(&web),
            ),
            (
                Method::GET,
                format!("{blog}/environments/staging/workloads/web"),
                None,
            ),
            (
                Method::GET,
                format!("{SHOP}/environments/dev/workloads/web"),
                None,
            ),
            (Method::GET, format!("{STAGING}/workloads/web"), None),
            (Method::DELETE, format!("{STAGING}/workloads/web"), None),
            // Routes the API does not have.
            (Method::GET, format!("{SHOP}/environments"), None),
            (Method::GET, format!("{STAGING}/workloads"), None),
            (Method::GET, "/api/v1/workloads/web".to_string(), None),
        ];
        for (method, uri, body) in cases {
            assert_eq!(
                refused(&node, method.clone(), &uri, body).await,
                (404, error_code::NOT_FOUND.into()),
                "{method} {uri}"
            );
        }
        node.shutdown().await;
    }

    /// A 404 says what is missing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn not_found_names_what_is_missing() {
        let node = TestNode::start_initialized().await;
        create_shop(&node).await;
        let web = json!({"workloads": {"web": spec("nginx")}});
        let blog = "/api/v1/projects/blog";
        let no_project = r#"project "blog" does not exist"#;
        let no_environment = r#"environment "dev" of project "shop" does not exist"#;
        let cases = [
            (Method::GET, blog.to_string(), None, no_project),
            (Method::DELETE, blog.to_string(), None, no_project),
            (
                Method::PUT,
                format!("{blog}/environments/dev"),
                None,
                no_project,
            ),
            (
                Method::POST,
                format!("{blog}/environments/dev/deploy"),
                Some(&web),
                no_project,
            ),
            (
                Method::DELETE,
                format!("{SHOP}/environments/dev"),
                None,
                no_environment,
            ),
            (
                Method::POST,
                format!("{SHOP}/environments/dev/deploy"),
                Some(&web),
                no_environment,
            ),
            (
                Method::GET,
                format!("{SHOP}/environments/dev/workloads/web"),
                None,
                no_environment,
            ),
            (
                Method::GET,
                format!("{STAGING}/workloads/web"),
                None,
                r#"workload "web" of environment "staging" of project "shop" does not exist"#,
            ),
            (
                Method::DELETE,
                format!("{STAGING}/workloads/web"),
                None,
                r#"workload "web" does not exist"#,
            ),
        ];
        for (method, uri, body, want) in cases {
            let (status, err): (_, ErrorBody) = call(&node, method.clone(), &uri, body).await;
            assert_eq!(
                (status, err.error.as_str(), err.message.as_deref()),
                (404, error_code::NOT_FOUND, Some(want)),
                "{method} {uri}"
            );
        }
        node.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn conflicts_are_409_with_their_code() {
        let node = TestNode::start_initialized().await;
        create_shop(&node).await;
        let p = "/api/v1/projects";

        // `project_exists`: the project, and an environment it has.
        for body in [
            json!({"name": "shop"}),
            json!({"name": "shop", "environments": ["dev"]}),
        ] {
            assert_eq!(
                refused(&node, Method::POST, p, Some(&body)).await,
                (409, error_code::PROJECT_EXISTS.into())
            );
        }
        assert_eq!(
            refused(&node, Method::PUT, STAGING, None).await,
            (409, error_code::PROJECT_EXISTS.into())
        );

        // `not_empty`: an environment with a workload, and its project.
        deploy_to(&node, STAGING, json!({"workloads": {"web": spec("nginx")}})).await;
        for uri in [STAGING, SHOP] {
            assert_eq!(
                refused(&node, Method::DELETE, uri, None).await,
                (409, error_code::NOT_EMPTY.into()),
                "{uri}"
            );
        }

        // `workloads_exhausted`: a project that is full of environments.
        let sixteen: Vec<String> = (0..limits::MAX_ENVIRONMENTS_PER_PROJECT)
            .map(|n| format!("e{n}"))
            .collect();
        let full = json!({"name": "full", "environments": sixteen});
        let (status, _): (_, ProjectResponse) = call(&node, Method::POST, p, Some(&full)).await;
        assert_eq!(status, 201);
        let one_more = format!("{p}/full/environments/one-more");
        assert_eq!(
            refused(&node, Method::PUT, &one_more, None).await,
            (409, error_code::WORKLOADS_EXHAUSTED.into())
        );
        node.shutdown().await;
    }

    /// The internal route on the peer listener is what a follower forwards
    /// to. It validates like the public routes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_peer_workloads_route_takes_the_six_writes() {
        let node = TestNode::start_initialized().await;
        let post_write = |body: Value, forwarded: bool| {
            let mut req = request(
                Method::POST,
                &node.peer_addr.to_string(),
                "/raft/cluster/workloads",
                Some(body.to_string()),
            );
            if forwarded {
                req.headers_mut()
                    .insert(FORWARDED_HEADER, "1".parse().unwrap());
            }
            req
        };
        let apply = |spec: Value| {
            json!({"op": "apply_workloads", "project": "shop", "environment": "staging",
                   "workloads": {"web": spec}, "prune": false})
        };

        let create = json!({"op": "create_project", "name": "shop",
                            "environments": ["staging"]});
        let (status, created): (_, PeerWorkloadResponse) =
            send(peer(&node), post_write(create.clone(), true)).await;
        assert_eq!((status, created.applied), (200, None));
        assert!(created.index > 0);
        // The leader sends the project along for the follower's reply.
        let project = created.project.unwrap();
        assert_eq!(project.name, "shop");
        assert_eq!(project.environments[0].generation, created.index);

        let (status, deployed): (_, PeerWorkloadResponse) =
            send(peer(&node), post_write(apply(spec("nginx")), false)).await;
        assert_eq!(status, 200);
        let applied = deployed.applied.unwrap();
        assert_eq!(applied.created, vec!["web"]);
        assert_eq!(applied.generation, deployed.index);
        assert_eq!(listed(&node, "").await, vec!["shop/staging/web"]);

        // An unchanged deploy is not written: `index` is the entry that
        // last changed the environment, which is its generation.
        let (status, again): (_, PeerWorkloadResponse) =
            send(peer(&node), post_write(apply(spec("nginx")), true)).await;
        assert_eq!(status, 200);
        assert_eq!(again.index, deployed.index);
        assert_eq!(again.applied.unwrap().generation, deployed.index);

        // The leader runs the validation itself, forwarded or not.
        for forwarded in [false, true] {
            for bad in [
                apply(json!({"containers": []})),
                apply(json!({"containers": [{"name": "main", "image": "nginx", "tty": true}]})),
                json!({"op": "create_project", "name": "Blog", "environments": ["dev"]}),
                json!({"op": "create_project", "name": "blog", "environments": []}),
                json!({"op": "delete_project", "name": "../shop"}),
                json!({"op": "set", "key": "projects", "value": {}}),
                json!({"name": "blog"}),
            ] {
                let (status, err): (_, ErrorBody) =
                    send(peer(&node), post_write(bad.clone(), forwarded)).await;
                assert_eq!(
                    (status, err.error.as_str()),
                    (400, error_code::BAD_REQUEST),
                    "{bad}"
                );
            }
        }
        let (status, err): (_, ErrorBody) = send(peer(&node), post_write(create, true)).await;
        assert_eq!(
            (status, err.error.as_str()),
            (409, error_code::PROJECT_EXISTS)
        );
        assert_eq!(listed(&node, "").await, vec!["shop/staging/web"]);

        // The loopback listener has no internal routes.
        let mut req = post_write(apply(spec("nginx:2")), false);
        req.headers_mut()
            .insert(header::HOST, "127.0.0.1:7400".parse().unwrap());
        let resp = local(&node).oneshot(req).await.unwrap();
        assert_eq!(resp.status().as_u16(), 404);
        let stored = node.service.workload("shop", "staging", "web").await;
        assert_eq!(stored.unwrap().spec.containers[0].image, "nginx");
        node.shutdown().await;
    }
}
