//! Stream implementation for PipeWire.

use crate::traits::StreamTrait;
use crate::{
    BackendSpecificError, BuildStreamError, Data, InputCallbackInfo, OutputCallbackInfo,
    PauseStreamError, PlayStreamError, SampleFormat, StreamConfig, StreamError,
};
use pipewire::spa::param::ParamType;
use pipewire::spa::pod::Pod;
use pipewire::stream::StreamState;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::PipeWireContext;

/// A PipeWire audio stream.
pub struct Stream {
    stream: pipewire::stream::StreamRc,
    playing: Arc<AtomicBool>,
    _context: Arc<PipeWireContext>,
    _listener: Box<dyn std::any::Any>,
}

impl Stream {
    /// Creates a new input (capture) stream.
    pub(crate) fn new_input<D, E>(
        context: Arc<PipeWireContext>,
        node_id: u32,
        config: &StreamConfig,
        sample_format: SampleFormat,
        mut data_callback: D,
        mut error_callback: E,
        _timeout: Option<std::time::Duration>,
    ) -> Result<Self, BuildStreamError>
    where
        D: FnMut(&Data, &InputCallbackInfo) + Send + 'static,
        E: FnMut(StreamError) + Send + 'static,
    {
        // Create stream with properties
        let stream = pipewire::stream::StreamRc::new(
            context.core.clone(),
            "cpal-input",
            pipewire::properties::properties! {
                *pipewire::keys::MEDIA_TYPE => "Audio",
                *pipewire::keys::MEDIA_CATEGORY => "Capture",
                *pipewire::keys::MEDIA_ROLE => "Music",
            },
        )
        .map_err(|e| BuildStreamError::BackendSpecific {
            err: BackendSpecificError {
                description: format!("Failed to create PipeWire stream: {}", e),
            },
        })?;

        let playing = Arc::new(AtomicBool::new(false));
        let playing_clone = Arc::clone(&playing);

        // Set up process callback
        let listener = stream
            .add_local_listener_with_user_data(())
            .process(move |stream, _| {
                if !playing_clone.load(Ordering::Acquire) {
                    return;
                }

                // Dequeue buffer
                let Some(mut buffer) = stream.dequeue_buffer() else {
                    return;
                };

                // Get buffer data
                let datas = buffer.datas_mut();
                if datas.is_empty() {
                    return;
                }

                let data_ref = &mut datas[0];
                let chunk = data_ref.chunk();
                let size = chunk.size() as usize;

                if size == 0 {
                    return;
                }

                // Get data slice
                let Some(slice) = data_ref.data() else {
                    return;
                };
                let slice = &slice[..size];

                // Wrap as CPAL Data and call user callback
                let data = unsafe { data_from_slice(slice, sample_format) };
                let info = InputCallbackInfo {
                    timestamp: crate::InputStreamTimestamp {
                        callback: crate::StreamInstant::from_secs_f64(0.0),
                        capture: crate::StreamInstant::from_secs_f64(0.0),
                    },
                };

                data_callback(&data, &info);
            })
            .state_changed(move |_, _, _old, new| {
                if matches!(new, StreamState::Error(_)) {
                    error_callback(StreamError::DeviceNotAvailable);
                }
            })
            .register()
            .map_err(|e| BuildStreamError::BackendSpecific {
                err: BackendSpecificError {
                    description: format!("Failed to register stream listener: {}", e),
                },
            })?;

        // Build audio format parameters
        let param_vec = build_audio_param(config, sample_format)?;
        let mut params = [Pod::from_bytes(&param_vec).unwrap()];

        // Connect stream to target node
        stream
            .connect(
                pipewire::spa::utils::Direction::Input,
                Some(node_id),
                pipewire::stream::StreamFlags::AUTOCONNECT
                    | pipewire::stream::StreamFlags::MAP_BUFFERS
                    | pipewire::stream::StreamFlags::RT_PROCESS,
                &mut params,
            )
            .map_err(|e| BuildStreamError::BackendSpecific {
                err: BackendSpecificError {
                    description: format!("Failed to connect stream: {}", e),
                },
            })?;

        Ok(Stream {
            stream,
            playing,
            _context: context,
            _listener: Box::new(listener),
        })
    }

    /// Creates a new output (playback) stream.
    pub(crate) fn new_output<D, E>(
        context: Arc<PipeWireContext>,
        node_id: u32,
        config: &StreamConfig,
        sample_format: SampleFormat,
        mut data_callback: D,
        mut error_callback: E,
        _timeout: Option<std::time::Duration>,
    ) -> Result<Self, BuildStreamError>
    where
        D: FnMut(&mut Data, &OutputCallbackInfo) + Send + 'static,
        E: FnMut(StreamError) + Send + 'static,
    {
        // Detect DoP streams (DSD over PCM) by sample rate
        // DoP uses 176.4kHz (DSD64) or 352.8kHz (DSD128)
        let sample_rate_hz = config.sample_rate.0;
        let is_dop = matches!(sample_rate_hz, 176400 | 352800) &&
                     matches!(sample_format, SampleFormat::I24 | SampleFormat::I32);

        // Create stream with properties
        // For DoP streams, add properties to disable resampling and processing
        let mut props = pipewire::properties::properties! {
            *pipewire::keys::MEDIA_TYPE => "Audio",
            *pipewire::keys::MEDIA_CATEGORY => "Playback",
            *pipewire::keys::MEDIA_ROLE => "Music",
        };

        if is_dop {
            // Add DoP-specific properties to prevent marker corruption
            props.insert("node.rate", sample_rate_hz.to_string());
            props.insert("resample.disable", "true".to_string());
            props.insert("node.dont-remix", "true".to_string());
            props.insert("stream.dont-remix", "true".to_string());
            props.insert("node.want-driver", "true".to_string());
        }

        let stream = pipewire::stream::StreamRc::new(
            context.core.clone(),
            if is_dop { "cpal-dop-output" } else { "cpal-output" },
            props,
        )
        .map_err(|e| BuildStreamError::BackendSpecific {
            err: BackendSpecificError {
                description: format!("Failed to create PipeWire stream: {}", e),
            },
        })?;

        let playing = Arc::new(AtomicBool::new(false));
        let playing_clone = Arc::clone(&playing);

        // Set up process callback
        let listener = stream
            .add_local_listener_with_user_data(())
            .process(move |stream, _| {
                if !playing_clone.load(Ordering::Acquire) {
                    return;
                }

                // Dequeue buffer
                let Some(mut buffer) = stream.dequeue_buffer() else {
                    return;
                };

                // Get buffer data
                let datas = buffer.datas_mut();
                if datas.is_empty() {
                    return;
                }

                let data_ref = &mut datas[0];
                let chunk = data_ref.chunk();
                let max_size = chunk.size() as usize;

                if max_size == 0 {
                    return;
                }

                // Get mutable data slice
                let Some(slice) = data_ref.data() else {
                    return;
                };
                let slice = &mut slice[..max_size];

                // Wrap as CPAL Data and call user callback
                let mut data = unsafe { data_from_slice_mut(slice, sample_format) };
                let info = OutputCallbackInfo {
                    timestamp: crate::OutputStreamTimestamp {
                        callback: crate::StreamInstant::from_secs_f64(0.0),
                        playback: crate::StreamInstant::from_secs_f64(0.0),
                    },
                };

                data_callback(&mut data, &info);

                // Update chunk size to actual written size
                let chunk_mut = data_ref.chunk_mut();
                *chunk_mut.size_mut() = max_size as u32;
                *chunk_mut.offset_mut() = 0;
            })
            .state_changed(move |_, _, _old, new| {
                if matches!(new, StreamState::Error(_)) {
                    error_callback(StreamError::DeviceNotAvailable);
                }
            })
            .register()
            .map_err(|e| BuildStreamError::BackendSpecific {
                err: BackendSpecificError {
                    description: format!("Failed to register stream listener: {}", e),
                },
            })?;

        // Build audio format parameters
        let param_vec = build_audio_param(config, sample_format)?;
        let mut params = [Pod::from_bytes(&param_vec).unwrap()];

        // Connect stream to target node
        stream
            .connect(
                pipewire::spa::utils::Direction::Output,
                Some(node_id),
                pipewire::stream::StreamFlags::AUTOCONNECT
                    | pipewire::stream::StreamFlags::MAP_BUFFERS
                    | pipewire::stream::StreamFlags::RT_PROCESS,
                &mut params,
            )
            .map_err(|e| BuildStreamError::BackendSpecific {
                err: BackendSpecificError {
                    description: format!("Failed to connect stream: {}", e),
                },
            })?;

        Ok(Stream {
            stream,
            playing,
            _context: context,
            _listener: Box::new(listener),
        })
    }
}

impl StreamTrait for Stream {
    fn play(&self) -> Result<(), PlayStreamError> {
        self.playing.store(true, Ordering::Release);

        self.stream
            .set_active(true)
            .map_err(|e| PlayStreamError::BackendSpecific {
                err: BackendSpecificError {
                    description: format!("Failed to activate PipeWire stream: {}", e),
                },
            })?;

        Ok(())
    }

    fn pause(&self) -> Result<(), PauseStreamError> {
        self.playing.store(false, Ordering::Release);

        self.stream
            .set_active(false)
            .map_err(|e| PauseStreamError::BackendSpecific {
                err: BackendSpecificError {
                    description: format!("Failed to deactivate PipeWire stream: {}", e),
                },
            })?;

        Ok(())
    }
}

/// Builds audio format parameter Pod bytes from stream config.
fn build_audio_param(
    config: &StreamConfig,
    sample_format: SampleFormat,
) -> Result<Vec<u8>, BuildStreamError> {
    use pipewire::spa::param::audio::AudioInfoRaw;

    let format = sample_format_to_pw_format(sample_format)?;

    let mut audio_info = AudioInfoRaw::new();
    audio_info.set_format(format);
    audio_info.set_rate(config.sample_rate);
    audio_info.set_channels(config.channels as u32);

    // Convert audio info to POD
    let obj = pipewire::spa::pod::Object {
        type_: pipewire::spa::utils::SpaTypes::ObjectParamFormat.as_raw(),
        id: ParamType::EnumFormat.as_raw(),
        properties: audio_info.into(),
    };

    let values: Vec<u8> = pipewire::spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &pipewire::spa::pod::Value::Object(obj),
    )
    .map_err(|e| BuildStreamError::BackendSpecific {
        err: BackendSpecificError {
            description: format!("Failed to serialize audio format: {:?}", e),
        },
    })?
    .0
    .into_inner();

    Ok(values)
}

/// Converts CPAL sample format to PipeWire audio format.
fn sample_format_to_pw_format(
    format: SampleFormat,
) -> Result<pipewire::spa::param::audio::AudioFormat, BuildStreamError> {
    use pipewire::spa::param::audio::AudioFormat;

    let pw_format = match format {
        SampleFormat::I8 => AudioFormat::S8,
        SampleFormat::I16 => AudioFormat::S16LE,
        SampleFormat::I32 => AudioFormat::S32LE,
        SampleFormat::I64 => return Err(BuildStreamError::StreamConfigNotSupported), // Not supported
        SampleFormat::U8 => AudioFormat::U8,
        SampleFormat::U16 => AudioFormat::U16LE,
        SampleFormat::U32 => AudioFormat::U32LE,
        SampleFormat::U64 => return Err(BuildStreamError::StreamConfigNotSupported), // Not supported
        SampleFormat::F32 => AudioFormat::F32LE,
        SampleFormat::F64 => AudioFormat::F64LE,
        _ => return Err(BuildStreamError::StreamConfigNotSupported),
    };

    Ok(pw_format)
}

/// Creates a CPAL Data struct from a byte slice.
unsafe fn data_from_slice(slice: &[u8], format: SampleFormat) -> Data {
    let ptr = slice.as_ptr() as *mut ();
    let len = match format {
        SampleFormat::I8 | SampleFormat::U8 => slice.len(),
        SampleFormat::I16 | SampleFormat::U16 => slice.len() / 2,
        SampleFormat::I32 | SampleFormat::U32 | SampleFormat::F32 => slice.len() / 4,
        SampleFormat::I64 | SampleFormat::U64 | SampleFormat::F64 => slice.len() / 8,
        _ => slice.len(),
    };
    Data::from_parts(ptr, len, format)
}

/// Creates a mutable CPAL Data struct from a mutable byte slice.
unsafe fn data_from_slice_mut(slice: &mut [u8], format: SampleFormat) -> Data {
    let ptr = slice.as_mut_ptr() as *mut ();
    let len = match format {
        SampleFormat::I8 | SampleFormat::U8 => slice.len(),
        SampleFormat::I16 | SampleFormat::U16 => slice.len() / 2,
        SampleFormat::I32 | SampleFormat::U32 | SampleFormat::F32 => slice.len() / 4,
        SampleFormat::I64 | SampleFormat::U64 | SampleFormat::F64 => slice.len() / 8,
        _ => slice.len(),
    };
    Data::from_parts(ptr, len, format)
}
