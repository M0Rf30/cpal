//! PipeWire backend implementation.
//!
//! Available on Linux, DragonFly BSD, FreeBSD, and NetBSD with the `pipewire` feature.
//! Requires PipeWire daemon running on the system.
//!
//! PipeWire is a modern multimedia server that provides low-latency audio with automatic
//! format conversion and better hardware integration. It is the default audio server on
//! many modern Linux distributions.

use crate::traits::HostTrait;
use crate::DevicesError;
use std::sync::{Arc, Mutex};

mod device;
mod registry;
mod stream;

#[allow(unused_imports)] // Re-exported for public API via platform module
pub use self::{
    device::{Device, SupportedInputConfigs, SupportedOutputConfigs},
    stream::Stream,
};

pub type Devices = std::vec::IntoIter<Device>;

/// Internal context shared between Host, Device, and Stream.
/// Contains the PipeWire main loop, core, and registry state.
pub(crate) struct PipeWireContext {
    _main_loop: pipewire::main_loop::MainLoopRc,
    core: pipewire::core::CoreRc,
    _registry: pipewire::registry::RegistryRc,
    _registry_listener: pipewire::registry::Listener,
    registry_state: Arc<Mutex<registry::RegistryState>>,
}

// SAFETY: PipeWireContext can be safely shared across threads because:
// 1. The PipeWire main loop manages its own internal threading and synchronization
// 2. We never directly access the PipeWire objects from multiple threads concurrently
// 3. CoreRc is only cloned (not accessed) from Device/Stream creation
// 4. The only shared mutable state (registry_state) is protected by Arc<Mutex<>>
// 5. All PipeWire callbacks run on PipeWire's own managed threads
unsafe impl Send for PipeWireContext {}
unsafe impl Sync for PipeWireContext {}

impl PipeWireContext {
    fn new() -> Result<Self, crate::HostUnavailable> {
        // Initialize PipeWire
        pipewire::init();

        // Create main loop
        let main_loop =
            pipewire::main_loop::MainLoopRc::new(None).map_err(|_| crate::HostUnavailable)?;

        // Create context
        let context = pipewire::context::ContextRc::new(&main_loop, None)
            .map_err(|_| crate::HostUnavailable)?;

        // Connect to PipeWire daemon
        let core = context
            .connect_rc(None)
            .map_err(|_| crate::HostUnavailable)?;

        // Initialize registry state
        let registry_state = Arc::new(Mutex::new(registry::RegistryState::new()));

        // Set up registry listener
        let (registry, registry_listener) =
            registry::setup_registry(&core, Arc::clone(&registry_state))
                .map_err(|_| crate::HostUnavailable)?;

        // Perform a sync to ensure we receive all existing globals
        let _pending = core.sync(0).map_err(|_| crate::HostUnavailable)?;

        // Run loop multiple times to populate initial devices
        // PipeWire needs time to enumerate devices via registry events
        for _ in 0..20 {
            main_loop
                .loop_()
                .iterate(std::time::Duration::from_millis(10));
        }

        Ok(PipeWireContext {
            _main_loop: main_loop,
            core,
            _registry: registry,
            _registry_listener: registry_listener,
            registry_state,
        })
    }
}

/// The PipeWire host, providing access to PipeWire audio devices.
///
/// PipeWire is a modern multimedia server that handles audio, video, and MIDI on Linux systems.
/// It provides lower latency than PulseAudio and automatic format conversion, making it easier
/// to work with diverse hardware configurations.
///
/// # Availability
///
/// This backend requires:
/// - PipeWire daemon running on the system
/// - `pipewire` feature enabled at compile time
///
/// Use [`is_available()`](Host::is_available) to check if PipeWire is accessible at runtime.
#[derive(Clone)]
pub struct Host {
    inner: Arc<PipeWireContext>,
}

impl Host {
    pub fn new() -> Result<Self, crate::HostUnavailable> {
        let context = PipeWireContext::new()?;
        Ok(Host {
            inner: Arc::new(context),
        })
    }

    /// Checks if the PipeWire backend is available on this system.
    ///
    /// Returns `true` if the PipeWire daemon is running and accessible, `false` otherwise.
    ///
    /// Unlike the JACK backend (which always returns `true`), this performs an actual
    /// runtime check by attempting to connect to the PipeWire daemon.
    pub fn is_available() -> bool {
        Self::new().is_ok()
    }
}

impl HostTrait for Host {
    type Devices = Devices;
    type Device = Device;

    fn is_available() -> bool {
        Host::is_available()
    }

    fn devices(&self) -> Result<Self::Devices, DevicesError> {
        let registry_state = self.inner.registry_state.lock().unwrap();
        let devices: Vec<Device> = registry_state
            .nodes
            .values()
            .map(|node_info| Device::from_node_info(node_info.clone(), Arc::clone(&self.inner)))
            .collect();
        Ok(devices.into_iter())
    }

    fn default_input_device(&self) -> Option<Self::Device> {
        let registry_state = self.inner.registry_state.lock().unwrap();
        registry_state
            .default_source_id
            .and_then(|id| registry_state.nodes.get(&id))
            .map(|node_info| Device::from_node_info(node_info.clone(), Arc::clone(&self.inner)))
    }

    fn default_output_device(&self) -> Option<Self::Device> {
        let registry_state = self.inner.registry_state.lock().unwrap();
        registry_state
            .default_sink_id
            .and_then(|id| registry_state.nodes.get(&id))
            .map(|node_info| Device::from_node_info(node_info.clone(), Arc::clone(&self.inner)))
    }
}
