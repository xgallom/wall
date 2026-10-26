use std::str;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::errors::PipelineError;
use crate::models::{LogLevel, LogMessage, Parsed, ProcessedLogView, Raw};

pub async fn handle_client_stream(
    stream: TcpStream,
    tx: mpsc::Sender<LogMessage<Raw>>,
    shutdown_token: CancellationToken,
) -> Result<(), PipelineError> {
    let mut reader = BufReader::new(stream);
    let mut buffer = String::new();

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                break;
            }
            read_res = reader.read_line(&mut buffer) => {
                let bytes_read = read_res?;
                if bytes_read == 0 {
                    break; // Client disconnected
                }

                let trimmed = buffer.trim_end();
                if !trimmed.is_empty() {
                    let raw_payload = trimmed.as_bytes().to_vec();
                    let raw_message = LogMessage::<Raw>::new(raw_payload);

                    if tx.send(raw_message).await.is_err() {
                        // Receiver channel dropped (system shutting down)
                        break;
                    }
                }
                buffer.clear();
            }
        }
    }
    Ok(())
}

pub fn parse_raw_message(raw_msg: &LogMessage<Raw>) -> Result<LogMessage<Parsed>, PipelineError> {
    let _ = str::from_utf8(&raw_msg.payload)
        .map_err(|_| PipelineError::ParseError("Invalid UTF-8 sequence".to_string()))?;

    let raw_clone = LogMessage::<Raw>::new(raw_msg.payload.clone());
    Ok(raw_clone.into_parsed())
}

pub fn extract_view<'a>(payload: &'a [u8]) -> Result<ProcessedLogView<'a>, PipelineError> {
    let raw_str = str::from_utf8(payload)
        .map_err(|_| PipelineError::ParseError("Invalid UTF-8 in viewport".to_string()))?;

    if !raw_str.starts_with('[') {
        return Err(PipelineError::ParseError("Missing opening bracket for log level".to_string()));
    }

    let end_bracket = raw_str.find(']')
        .ok_or_else(
            || PipelineError::ParseError("Missing closing bracket for log level".to_string())
        )?;

    let level_str = &raw_str[1..end_bracket];
    let level = match level_str.to_uppercase().as_str() {
        "DEBUG" => LogLevel::Debug,
        "INFO"  => LogLevel::Info,
        "WARN"  => LogLevel::Warn,
        "ERROR" => LogLevel::Error,
        _       => return Err(PipelineError::ParseError(format!("Unknown log level: {}", level_str))),
    };

    let remainder = &raw_str[end_bracket + 1..].trim_start();
    let delimiter = " :: ";

    let delim_idx = remainder.find(delimiter)
        .ok_or_else(|| PipelineError::ParseError("Missing component delimiter ' :: '".to_string()))?;

    let component = &remainder[..delim_idx].trim();
    let body = &remainder[delim_idx + delimiter.len()..].trim();

    Ok(ProcessedLogView {
        level,
        component,
        body,
    })
}

