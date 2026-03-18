//! MCP resource handlers — exposes active connection profiles as browsable resources.
//!
//! Each profile that has been connected at least once is surfaced as a resource
//! URI of the form `arni://profiles/{name}`, allowing MCP clients to list and
//! inspect live database connections without calling a tool.

use serde_json::json;

use arni::ConnectionRegistry;

/// Return all active profile names as serializable resource descriptors.
///
/// Profiles appear here only after a first successful connection. Call `query`
/// or any other tool once to warm the connection, then list resources to
/// confirm availability.
pub fn list_profile_resources(registry: &ConnectionRegistry) -> serde_json::Value {
    let names = registry.active_profiles();
    let resources: Vec<serde_json::Value> = names
        .iter()
        .map(|name| {
            json!({
                "uri": format!("arni://profiles/{}", name),
                "name": name,
                "description": format!("Live database connection: {}", name),
                "mimeType": "application/json",
            })
        })
        .collect();
    json!({ "resources": resources })
}

/// Read a single profile resource by URI, returning its status as JSON.
pub fn read_profile_resource(
    registry: &ConnectionRegistry,
    uri: &str,
) -> Result<serde_json::Value, String> {
    let name = uri
        .strip_prefix("arni://profiles/")
        .ok_or_else(|| format!("Unrecognised resource URI: {}", uri))?;

    let active = registry.active_profiles();
    if !active.contains(&name.to_string()) {
        return Err(format!("Profile '{}' not found or not yet connected", name));
    }

    Ok(json!({
        "uri": uri,
        "mimeType": "application/json",
        "text": json!({ "profile": name, "status": "connected" }).to_string(),
    }))
}
