//! Device enumeration and configuration for PipeWire.

use crate::traits::DeviceTrait;
use crate::{
    BuildStreamError, Data, DefaultStreamConfigError, DeviceDescription, DeviceDescriptionBuilder,
    DeviceDirection, DeviceId, DeviceIdError, DeviceNameError, InputCallbackInfo,
    OutputCallbackInfo, SampleFormat, StreamConfig, StreamError, SupportedBufferSize,
    SupportedStreamConfig, SupportedStreamConfigRange, SupportedStreamConfigsError,
};
use std::sync::Arc;

use super::registry::NodeInfo;
use super::stream::Stream;
use super::PipeWireContext;

/// A PipeWire audio device (node).
#[derive(Clone)]
pub struct Device {
    node_info: NodeInfo,
    context: Arc<PipeWireContext>,
}

impl Device {
    pub(crate) fn from_node_info(node_info: NodeInfo, context: Arc<PipeWireContext>) -> Self {
        Device { node_info, context }
    }

    /// Returns the unique identifier for this device.
    fn id(&self) -> Result<DeviceId, DeviceIdError> {
        Ok(DeviceId(
            crate::platform::HostId::PipeWire,
            format!("pipewire-node-{}", self.node_info.id),
        ))
    }
}

impl DeviceTrait for Device {
    type SupportedInputConfigs = SupportedInputConfigs;
    type SupportedOutputConfigs = SupportedOutputConfigs;
    type Stream = Stream;

    fn name(&self) -> Result<String, DeviceNameError> {
        Ok(self.node_info.description.clone())
    }

    fn description(&self) -> Result<DeviceDescription, DeviceNameError> {
        let direction = if self.node_info.is_input {
            DeviceDirection::Input
        } else {
            DeviceDirection::Output
        };

        Ok(
            DeviceDescriptionBuilder::new(self.node_info.description.clone())
                .direction(direction)
                .build(),
        )
    }

    fn id(&self) -> Result<DeviceId, DeviceIdError> {
        Device::id(self)
    }

    fn supported_input_configs(
        &self,
    ) -> Result<Self::SupportedInputConfigs, SupportedStreamConfigsError> {
        if !self.node_info.is_input {
            return Err(SupportedStreamConfigsError::DeviceNotAvailable);
        }

        // PipeWire handles format conversion automatically, so we report all formats
        // that CPAL supports. Let PipeWire do the heavy lifting.
        let formats = vec![
            SampleFormat::I8,
            SampleFormat::I16,
            SampleFormat::I32,
            SampleFormat::I64,
            SampleFormat::U8,
            SampleFormat::U16,
            SampleFormat::U32,
            SampleFormat::U64,
            SampleFormat::F32,
            SampleFormat::F64,
        ];

        let configs: Vec<SupportedStreamConfigRange> = formats
            .into_iter()
            .flat_map(|format| {
                // Support common channel configurations
                vec![1u16, 2, 4, 6, 8]
                    .into_iter()
                    .map(move |channels| SupportedStreamConfigRange {
                        channels,
                        min_sample_rate: 8000,
                        max_sample_rate: 192000,
                        buffer_size: SupportedBufferSize::Unknown,
                        sample_format: format,
                    })
            })
            .collect();

        Ok(SupportedInputConfigs(configs.into_iter()))
    }

    fn supported_output_configs(
        &self,
    ) -> Result<Self::SupportedOutputConfigs, SupportedStreamConfigsError> {
        if !self.node_info.is_output {
            return Err(SupportedStreamConfigsError::DeviceNotAvailable);
        }

        // Same as input: report all formats and let PipeWire handle conversion
        let formats = vec![
            SampleFormat::I8,
            SampleFormat::I16,
            SampleFormat::I32,
            SampleFormat::I64,
            SampleFormat::U8,
            SampleFormat::U16,
            SampleFormat::U32,
            SampleFormat::U64,
            SampleFormat::F32,
            SampleFormat::F64,
        ];

        let configs: Vec<SupportedStreamConfigRange> = formats
            .into_iter()
            .flat_map(|format| {
                // Support common channel configurations
                vec![1u16, 2, 4, 6, 8]
                    .into_iter()
                    .map(move |channels| SupportedStreamConfigRange {
                        channels,
                        min_sample_rate: 8000,
                        max_sample_rate: 192000,
                        buffer_size: SupportedBufferSize::Unknown,
                        sample_format: format,
                    })
            })
            .collect();

        Ok(SupportedOutputConfigs(configs.into_iter()))
    }

    fn default_input_config(&self) -> Result<SupportedStreamConfig, DefaultStreamConfigError> {
        if !self.node_info.is_input {
            return Err(DefaultStreamConfigError::DeviceNotAvailable);
        }

        // Default to stereo F32 at 48kHz (most common configuration)
        Ok(SupportedStreamConfig {
            channels: 2,
            sample_rate: 48000,
            buffer_size: SupportedBufferSize::Unknown,
            sample_format: SampleFormat::F32,
        })
    }

    fn default_output_config(&self) -> Result<SupportedStreamConfig, DefaultStreamConfigError> {
        if !self.node_info.is_output {
            return Err(DefaultStreamConfigError::DeviceNotAvailable);
        }

        // Default to stereo F32 at 48kHz (most common configuration)
        Ok(SupportedStreamConfig {
            channels: 2,
            sample_rate: 48000,
            buffer_size: SupportedBufferSize::Unknown,
            sample_format: SampleFormat::F32,
        })
    }

    fn build_input_stream_raw<D, E>(
        &self,
        config: &StreamConfig,
        sample_format: SampleFormat,
        data_callback: D,
        error_callback: E,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self::Stream, BuildStreamError>
    where
        D: FnMut(&Data, &InputCallbackInfo) + Send + 'static,
        E: FnMut(StreamError) + Send + 'static,
    {
        if !self.node_info.is_input {
            return Err(BuildStreamError::DeviceNotAvailable);
        }

        Stream::new_input(
            Arc::clone(&self.context),
            self.node_info.id,
            config,
            sample_format,
            data_callback,
            error_callback,
            timeout,
        )
    }

    fn build_output_stream_raw<D, E>(
        &self,
        config: &StreamConfig,
        sample_format: SampleFormat,
        data_callback: D,
        error_callback: E,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self::Stream, BuildStreamError>
    where
        D: FnMut(&mut Data, &OutputCallbackInfo) + Send + 'static,
        E: FnMut(StreamError) + Send + 'static,
    {
        if !self.node_info.is_output {
            return Err(BuildStreamError::DeviceNotAvailable);
        }

        Stream::new_output(
            Arc::clone(&self.context),
            self.node_info.id,
            config,
            sample_format,
            data_callback,
            error_callback,
            timeout,
        )
    }
}

/// Iterator over supported input stream configurations.
#[derive(Clone)]
pub struct SupportedInputConfigs(std::vec::IntoIter<SupportedStreamConfigRange>);

impl Iterator for SupportedInputConfigs {
    type Item = SupportedStreamConfigRange;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

/// Iterator over supported output stream configurations.
#[derive(Clone)]
pub struct SupportedOutputConfigs(std::vec::IntoIter<SupportedStreamConfigRange>);

impl Iterator for SupportedOutputConfigs {
    type Item = SupportedStreamConfigRange;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
