use std::{convert::Infallible, sync::Arc};

use hyper::{
    server::Server,
    service::{make_service_fn, service_fn},
    Body, Method, Response, StatusCode,
};
use juniper::{
    // tests::fixtures::starwars::schema::{Database, Query},
    EmptyMutation,
    EmptySubscription,
    RootNode,
};
use simple_db::PinnedDatabase;
use types::{DatabaseW as Database, Query};

mod types;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let addr = ([127, 0, 0, 1], 3001).into();

    let db = Arc::new(Database(
        PinnedDatabase::load_from_disk("../db".to_string()).unwrap(),
    ));
    let root_node = Arc::new(RootNode::new(
        Query,
        EmptyMutation::<Database>::new(),
        EmptySubscription::<Database>::new(),
    ));

    let new_service = make_service_fn(move |_| {
        let root_node = root_node.clone();
        let ctx = db.clone();

        async {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let root_node = root_node.clone();
                let ctx = ctx.clone();
                async {
                    Ok::<_, Infallible>(match (req.method(), req.uri().path()) {
                        (&Method::GET, "/") => juniper_hyper::graphiql("/graphql", None).await,
                        (&Method::GET, "/playground") => {
                            juniper_hyper::playground("/playground", None).await
                        }
                        (&Method::GET, "/graphql") | (&Method::POST, "/graphql") => {
                            juniper_hyper::graphql(root_node, ctx, req).await
                        }
                        _ => {
                            let mut response = Response::new(Body::empty());
                            *response.status_mut() = StatusCode::NOT_FOUND;
                            response
                        }
                    })
                }
            }))
        }
    });

    let server = Server::bind(&addr).serve(new_service);
    println!("Listening on http://{}", addr);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e)
    }
}
