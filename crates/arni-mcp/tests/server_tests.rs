//! arni-mcp unit tests — tests that don't require live database connections.
//!
//! These verify:
//! - `ArniMcpServer` constructs without panicking
//! - `get_info()` returns the correct server name and protocol version
//! - `resources.rs` helpers behave correctly with an empty/populated registry
//! - Filter DSL parsing handles all operators correctly

use std::sync::Arc;

use arni::{ArniConfig, ConnectionRegistry};
use arni_mcp::ArniMcpServer;
use rmcp::model::ProtocolVersion;
use rmcp::ServerHandler;

fn make_server() -> ArniMcpServer {
    let registry = Arc::new(ConnectionRegistry::new());
    let config = Arc::new(ArniConfig::default());
    ArniMcpServer::new(registry, config)
}

// ── Construction ──────────────────────────────────────────────────────────────

#[test]
fn server_constructs_without_panic() {
    let _server = make_server();
}

#[test]
fn server_get_info_name_is_arni() {
    let server = make_server();
    let info = server.get_info();
    assert_eq!(info.server_info.name, "arni");
}

#[test]
fn server_get_info_protocol_version() {
    let server = make_server();
    let info = server.get_info();
    assert_eq!(info.protocol_version, ProtocolVersion::V_2024_11_05);
}

#[test]
fn server_get_info_has_instructions() {
    let server = make_server();
    let info = server.get_info();
    assert!(info.instructions.is_some());
    let instr = info.instructions.unwrap();
    assert!(instr.contains("arni"));
}

// ── Resources helper functions ────────────────────────────────────────────────

#[test]
fn list_profile_resources_empty_when_no_connections() {
    use arni_mcp::resources::list_profile_resources;
    let registry = ConnectionRegistry::new();
    let result = list_profile_resources(&registry);
    let resources = result["resources"].as_array().unwrap();
    assert!(
        resources.is_empty(),
        "Expected no resources before any connection"
    );
}

#[test]
fn read_profile_resource_unknown_uri_returns_err() {
    use arni_mcp::resources::read_profile_resource;
    let registry = ConnectionRegistry::new();
    let r = read_profile_resource(&registry, "arni://profiles/no-such-profile");
    assert!(r.is_err());
    assert!(r.unwrap_err().contains("not found"));
}

#[test]
fn read_profile_resource_bad_scheme_returns_err() {
    use arni_mcp::resources::read_profile_resource;
    let registry = ConnectionRegistry::new();
    let r = read_profile_resource(&registry, "https://example.com/not-arni");
    assert!(r.is_err());
    assert!(r.unwrap_err().contains("Unrecognised"));
}

// ── Filter DSL ────────────────────────────────────────────────────────────────

#[test]
fn filter_eq_parses() {
    use arni::FilterExpr;
    use arni_mcp::filter::parse_filter_value;
    let f = parse_filter_value(&serde_json::json!({"id": {"eq": 1}})).unwrap();
    assert!(matches!(f, FilterExpr::Eq(col, _) if col == "id"));
}

#[test]
fn filter_ne_parses() {
    use arni::FilterExpr;
    use arni_mcp::filter::parse_filter_value;
    let f = parse_filter_value(&serde_json::json!({"status": {"ne": "deleted"}})).unwrap();
    assert!(matches!(f, FilterExpr::Ne(col, _) if col == "status"));
}

#[test]
fn filter_gt_lt_parses() {
    use arni::FilterExpr;
    use arni_mcp::filter::parse_filter_value;
    let f_gt = parse_filter_value(&serde_json::json!({"age": {"gt": 18}})).unwrap();
    let f_lt = parse_filter_value(&serde_json::json!({"age": {"lt": 65}})).unwrap();
    assert!(matches!(f_gt, FilterExpr::Gt(col, _) if col == "age"));
    assert!(matches!(f_lt, FilterExpr::Lt(col, _) if col == "age"));
}

#[test]
fn filter_in_parses_with_correct_count() {
    use arni::FilterExpr;
    use arni_mcp::filter::parse_filter_value;
    let f = parse_filter_value(&serde_json::json!({"id": {"in": [1, 2, 3]}})).unwrap();
    assert!(matches!(f, FilterExpr::In(col, v) if col == "id" && v.len() == 3));
}

#[test]
fn filter_and_parses() {
    use arni::FilterExpr;
    use arni_mcp::filter::parse_filter_value;
    let f = parse_filter_value(
        &serde_json::json!({"and": [{"a": {"eq": 1}}, {"b": {"gt": 0}}]}),
    )
    .unwrap();
    assert!(matches!(f, FilterExpr::And(v) if v.len() == 2));
}

#[test]
fn filter_or_parses() {
    use arni::FilterExpr;
    use arni_mcp::filter::parse_filter_value;
    let f = parse_filter_value(
        &serde_json::json!({"or": [{"a": {"eq": 1}}, {"b": {"eq": 2}}]}),
    )
    .unwrap();
    assert!(matches!(f, FilterExpr::Or(v) if v.len() == 2));
}

#[test]
fn filter_not_parses() {
    use arni::FilterExpr;
    use arni_mcp::filter::parse_filter_value;
    let f =
        parse_filter_value(&serde_json::json!({"not": {"active": {"eq": false}}})).unwrap();
    assert!(matches!(f, FilterExpr::Not(_)));
}

#[test]
fn filter_is_null_and_is_not_null_parse() {
    use arni::FilterExpr;
    use arni_mcp::filter::parse_filter_value;
    let null = parse_filter_value(&serde_json::json!({"col": "is_null"})).unwrap();
    let not_null = parse_filter_value(&serde_json::json!({"col": "is_not_null"})).unwrap();
    assert!(matches!(null, FilterExpr::IsNull(_)));
    assert!(matches!(not_null, FilterExpr::IsNotNull(_)));
}

#[test]
fn filter_unknown_op_returns_err() {
    use arni_mcp::filter::parse_filter_value;
    let r = parse_filter_value(&serde_json::json!({"col": {"between": [1, 5]}}));
    assert!(r.is_err());
}

#[test]
fn filter_array_not_object_returns_err() {
    use arni_mcp::filter::parse_filter_value;
    let r = parse_filter_value(&serde_json::json!([1, 2, 3]));
    assert!(r.is_err());
}

#[test]
fn filter_multi_key_object_returns_err() {
    use arni_mcp::filter::parse_filter_value;
    let r = parse_filter_value(&serde_json::json!({"a": 1, "b": 2}));
    assert!(r.is_err());
    assert!(r.unwrap_err().to_string().contains("exactly one key"));
}
