//! Registry management for device enumeration and hotplug support.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Information about a PipeWire audio node (device).
#[derive(Clone, Debug)]
pub(crate) struct NodeInfo {
    pub id: u32,
    #[allow(dead_code)]
    pub name: String,
    pub description: String,
    #[allow(dead_code)]
    pub media_class: String,
    pub is_input: bool,
    pub is_output: bool,
}

/// Shared state tracking all discovered PipeWire nodes.
pub(crate) struct RegistryState {
    pub nodes: HashMap<u32, NodeInfo>,
    pub default_sink_id: Option<u32>,
    pub default_source_id: Option<u32>,
}

impl RegistryState {
    pub(crate) fn new() -> Self {
        RegistryState {
            nodes: HashMap::new(),
            default_sink_id: None,
            default_source_id: None,
        }
    }
}

/// Sets up the registry listener to track audio devices.
/// Returns the registry and listener which must be kept alive.
pub(crate) fn setup_registry(
    core: &pipewire::core::CoreRc,
    registry_state: Arc<Mutex<RegistryState>>,
) -> Result<(pipewire::registry::RegistryRc, pipewire::registry::Listener), pipewire::Error> {
    let registry = core.get_registry_rc()?;

    // Clone for each closure
    let state_global = Arc::clone(&registry_state);
    let state_global_remove = Arc::clone(&registry_state);

    let listener = registry
        .add_listener_local()
        .global(move |global| {
            handle_global_event(global, &state_global);
        })
        .global_remove(move |id| {
            handle_global_remove_event(id, &state_global_remove);
        })
        .register();

    Ok((registry, listener))
}

/// Handles a new global object appearing in the registry.
fn handle_global_event(
    global: &pipewire::registry::GlobalObject<&pipewire::spa::utils::dict::DictRef>,
    registry_state: &Arc<Mutex<RegistryState>>,
) {
    // We only care about Node objects
    if global.type_ != pipewire::types::ObjectType::Node {
        return;
    }

    // Extract properties
    let props = global.props.as_ref();

    // Get media class to determine if this is an audio source or sink
    let media_class = props.and_then(|p| p.get("media.class")).unwrap_or("");

    // Filter for audio sources and sinks
    let (is_input, is_output) = match media_class {
        "Audio/Source" => (true, false),
        "Audio/Sink" => (false, true),
        _ => return, // Not an audio device we care about
    };

    // Get node name and description
    let name = props
        .and_then(|p| p.get("node.name"))
        .or_else(|| props.and_then(|p| p.get("object.path")))
        .unwrap_or("Unknown")
        .to_string();

    let description = props
        .and_then(|p| p.get("node.description"))
        .or_else(|| props.and_then(|p| p.get("node.nick")))
        .unwrap_or(&name)
        .to_string();

    let node_info = NodeInfo {
        id: global.id,
        name: name.clone(),
        description,
        media_class: media_class.to_string(),
        is_input,
        is_output,
    };

    // Update registry state
    let mut state = registry_state.lock().unwrap();
    state.nodes.insert(global.id, node_info);

    // Check if this is marked as default
    if let Some(props) = props {
        if let Some(is_default) = props.get("node.default") {
            if is_default == "true" {
                if is_input {
                    state.default_source_id = Some(global.id);
                } else if is_output {
                    state.default_sink_id = Some(global.id);
                }
            }
        }
    }

    // If we don't have a default yet, use the first device we find
    if is_input && state.default_source_id.is_none() {
        state.default_source_id = Some(global.id);
    }
    if is_output && state.default_sink_id.is_none() {
        state.default_sink_id = Some(global.id);
    }
}

/// Handles a global object being removed from the registry.
fn handle_global_remove_event(id: u32, registry_state: &Arc<Mutex<RegistryState>>) {
    let mut state = registry_state.lock().unwrap();

    // Remove the node
    state.nodes.remove(&id);

    // Clear default if this was the default device
    if state.default_sink_id == Some(id) {
        state.default_sink_id = None;
        // Set a new default if available
        if let Some((&new_id, _)) = state.nodes.iter().find(|(_, info)| info.is_output) {
            state.default_sink_id = Some(new_id);
        }
    }

    if state.default_source_id == Some(id) {
        state.default_source_id = None;
        // Set a new default if available
        if let Some((&new_id, _)) = state.nodes.iter().find(|(_, info)| info.is_input) {
            state.default_source_id = Some(new_id);
        }
    }
}
