use std::time::SystemTime;

pub trait LogState {}

pub struct Raw;
impl LogState for Raw {}

pub struct Parsed;
impl LogState for Parsed {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug)]
pub struct LogMessage<S: LogState> {
    pub timestamp: SystemTime,
    pub payload: Vec<u8>,
    pub state: std::marker::PhantomData<S>,
}

#[derive(Debug)]
pub struct ProcessedLogView<'a> {
    pub level: LogLevel,
    pub component: &'a str,
    pub body: &'a str,
}

impl LogMessage<Raw> {
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            timestamp: SystemTime::now(),
            payload,
            state: std::marker::PhantomData,
        }
    }

    pub fn into_parsed(self) -> LogMessage<Parsed> {
        LogMessage {
            timestamp: self.timestamp,
            payload: self.payload,
            state: std::marker::PhantomData,
        }
    }
}

impl LogMessage<Parsed> {
    pub fn view<'a>(&'a self) -> Result<ProcessedLogView<'a>, crate::errors::PipelineError> {
        crate::parser::extract_view(&self.payload)
    }
}

