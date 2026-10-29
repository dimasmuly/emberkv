//! End-to-end tests of the JSON/HTTP API over real sockets.

mod common;

use std::time::Duration;

use common::TestServer;
use reqwest::StatusCode;
use reqwest::header::{CONTENT_TYPE, ETAG, IF_MATCH, IF_NONE_MATCH};
use serde_json::{Value, json};

async fn error_code(response: reqwest::Response) -> String {
    let body: Value = response.json().await.unwrap();
    body["error"]["code"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn json_document_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;
    let http = reqwest::Client::new();
    let url = server.url("/v1/namespaces/profiles/keys/user:42");

    let missing = http.get(&url).send().await.unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(error_code(missing).await, "not_found");

    let put = http.put(&url).json(&json!({ "name": "Ada", "plan": "pro" })).send().await.unwrap();
    assert_eq!(put.status(), StatusCode::NO_CONTENT);
    let etag = put.headers()[ETAG].clone();

    let got = http.get(&url).send().await.unwrap();
    assert_eq!(got.status(), StatusCode::OK);
    assert_eq!(got.headers()[CONTENT_TYPE], "application/json");
    assert_eq!(got.headers()[ETAG], etag);
    assert_eq!(got.json::<Value>().await.unwrap(), json!({ "name": "Ada", "plan": "pro" }));

    let not_json = http.put(&url).body("definitely not json").send().await.unwrap();
    assert_eq!(not_json.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error_code(not_json).await, "invalid_json");

    assert_eq!(http.delete(&url).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    assert_eq!(http.delete(&url).send().await.unwrap().status(), StatusCode::NOT_FOUND);
    server.stop().await;
}

#[tokio::test]
async fn keys_expire_after_their_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;
    let http = reqwest::Client::new();
    let url = server.url("/v1/namespaces/otp/keys/user:42");

    let put = http.put(format!("{url}?ttl=300ms")).json(&json!({ "code": "481516" })).send().await.unwrap();
    assert_eq!(put.status(), StatusCode::NO_CONTENT);
    let got = http.get(&url).send().await.unwrap();
    assert_eq!(got.status(), StatusCode::OK);
    assert!(got.headers().contains_key("x-expires-at"));

    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(http.get(&url).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    let bad = http.put(format!("{url}?ttl=forever")).json(&json!(1)).send().await.unwrap();
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error_code(bad).await, "invalid_ttl");
    server.stop().await;
}

#[tokio::test]
async fn conditional_writes_prevent_lost_updates() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;
    let http = reqwest::Client::new();
    let url = server.url("/v1/namespaces/docs/keys/note");

    // Create-only: the second attempt must not overwrite.
    let create = || http.put(&url).header(IF_NONE_MATCH, "*").json(&json!({ "v": 1 }));
    assert_eq!(create().send().await.unwrap().status(), StatusCode::NO_CONTENT);
    let again = create().send().await.unwrap();
    assert_eq!(again.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(error_code(again).await, "precondition_failed");

    // Two writers read the same version; only the first update may win.
    let v1 = http.get(&url).send().await.unwrap().headers()[ETAG].clone();
    let first = http.put(&url).header(IF_MATCH, v1.clone()).json(&json!({ "v": 2 })).send().await.unwrap();
    assert_eq!(first.status(), StatusCode::NO_CONTENT);
    let second = http.put(&url).header(IF_MATCH, v1.clone()).json(&json!({ "v": 3 })).send().await.unwrap();
    assert_eq!(second.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(http.get(&url).send().await.unwrap().json::<Value>().await.unwrap(), json!({ "v": 2 }));

    let stale_delete = http.delete(&url).header(IF_MATCH, v1).send().await.unwrap();
    assert_eq!(stale_delete.status(), StatusCode::PRECONDITION_FAILED);
    let v2 = http.get(&url).send().await.unwrap().headers()[ETAG].clone();
    assert_eq!(http.delete(&url).header(IF_MATCH, v2).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    server.stop().await;
}

#[tokio::test]
async fn listing_paginates_and_stays_inside_the_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;
    let http = reqwest::Client::new();
    for i in 1..=5 {
        let url = server.url(&format!("/v1/namespaces/orders/keys/o{i}"));
        http.put(url).json(&json!({ "id": i })).send().await.unwrap();
    }
    http.put(server.url("/v1/namespaces/other/keys/o9")).json(&json!(0)).send().await.unwrap();

    let mut keys = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut url = server.url("/v1/namespaces/orders/keys?limit=2");
        if let Some(cursor) = &cursor {
            url.push_str(&format!("&cursor={cursor}"));
        }
        let page: Value = http.get(url).send().await.unwrap().json().await.unwrap();
        pages += 1;
        for item in page["items"].as_array().unwrap() {
            keys.push(item["key"].as_str().unwrap().to_owned());
            assert!(item["value"]["id"].is_number(), "values are embedded as JSON");
        }
        match page["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }
    assert_eq!(keys, ["o1", "o2", "o3", "o4", "o5"]);
    assert_eq!(pages, 3);

    let filtered: Value = http
        .get(server.url("/v1/namespaces/orders/keys?prefix=o3"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(filtered["items"].as_array().unwrap().len(), 1);
    server.stop().await;
}

#[tokio::test]
async fn batches_are_atomic_and_validated_up_front() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;
    let http = reqwest::Client::new();
    let key = |k: &str| server.url(&format!("/v1/namespaces/shop/keys/{k}"));
    http.put(key("cart:7")).json(&json!({ "items": 2 })).send().await.unwrap();

    let checkout = json!({ "operations": [
        { "op": "put", "key": "order:7", "value": { "status": "paid" } },
        { "op": "put", "key": "receipt:7", "value": { "total": 120 }, "ttl": "30d" },
        { "op": "delete", "key": "cart:7" },
    ]});
    let response = http.post(server.url("/v1/namespaces/shop/batch")).json(&checkout).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.json::<Value>().await.unwrap(), json!({ "applied": 3 }));
    assert_eq!(http.get(key("order:7")).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(http.get(key("cart:7")).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // An invalid operation rejects the whole batch: nothing is applied.
    let broken = json!({ "operations": [
        { "op": "put", "key": "order:8", "value": { "status": "paid" } },
        { "op": "put", "key": "receipt:8" },
    ]});
    let response = http.post(server.url("/v1/namespaces/shop/batch")).json(&broken).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error_code(response).await, "invalid_batch");
    assert_eq!(http.get(key("order:8")).send().await.unwrap().status(), StatusCode::NOT_FOUND);
    server.stop().await;
}

#[tokio::test]
async fn counters_and_rate_limits() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;
    let http = reqwest::Client::new();

    let counter = server.url("/v1/namespaces/stats/counters/page-views");
    let first: Value = http.post(&counter).send().await.unwrap().json().await.unwrap();
    assert_eq!(first["value"], 1);
    let second: Value =
        http.post(&counter).json(&json!({ "by": 5 })).send().await.unwrap().json().await.unwrap();
    assert_eq!(second["value"], 6);

    http.put(server.url("/v1/namespaces/stats/keys/name")).json(&json!("text")).send().await.unwrap();
    let not_counter = http.post(server.url("/v1/namespaces/stats/counters/name")).send().await.unwrap();
    assert_eq!(not_counter.status(), StatusCode::CONFLICT);
    assert_eq!(error_code(not_counter).await, "not_a_counter");

    // e.g. "at most 3 login attempts per minute per user"
    let limiter = server.url("/v1/namespaces/limits/rate-limits/login:user-42");
    let rule = json!({ "limit": 3, "window": "1m" });
    for remaining in [2, 1, 0] {
        let decision: Value = http.post(&limiter).json(&rule).send().await.unwrap().json().await.unwrap();
        assert_eq!(decision["allowed"], true);
        assert_eq!(decision["remaining"], remaining);
    }
    let denied: Value = http.post(&limiter).json(&rule).send().await.unwrap().json().await.unwrap();
    assert_eq!(denied["allowed"], false);
    assert!(denied["retry_after_ms"].as_u64().unwrap() <= 60_000);
    server.stop().await;
}

#[tokio::test]
async fn bad_input_gets_structured_errors() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;
    let http = reqwest::Client::new();

    let bad_ns = http.get(server.url("/v1/namespaces/Bad_NS/keys/x")).send().await.unwrap();
    assert_eq!(bad_ns.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error_code(bad_ns).await, "invalid_namespace");

    let no_route = http.get(server.url("/v2/whatever")).send().await.unwrap();
    assert_eq!(no_route.status(), StatusCode::NOT_FOUND);
    assert_eq!(error_code(no_route).await, "not_found");

    let health: Value = http.get(server.url("/healthz")).send().await.unwrap().json().await.unwrap();
    assert_eq!(health["status"], "ok");
    let ready = http.get(server.url("/readyz")).send().await.unwrap();
    assert_eq!(ready.status(), StatusCode::OK);
    server.stop().await;
}
