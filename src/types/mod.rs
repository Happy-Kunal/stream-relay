use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct Message {
    value: Bytes,
    offset: usize,
    topic_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Topic {
    name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TopicWithOffset {
    name: String,
    offset: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TopicMetaData {
    name: String,
    last_flushed_offset: usize,
    num_of_segments: usize,
}

impl Message {
    pub fn new(value: Bytes, offset: usize, topic_name: String) -> Self {
        Self {
            value,
            offset,
            topic_name,
        }
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub fn offset(&self) -> &usize {
        &self.offset
    }

    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }
}

impl Topic {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl TopicWithOffset {
    pub fn new(name: String, offset: usize) -> Self {
        Self { name, offset }
    }
}

impl TopicMetaData {
    pub fn new(name: String, last_flushed_offset: usize, num_of_segments: usize) -> Self {
        Self {
            name,
            last_flushed_offset,
            num_of_segments,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn last_flushed_offset(&self) -> &usize {
        &self.last_flushed_offset
    }

    pub fn num_of_segments(&self) -> &usize {
        &self.num_of_segments
    }
}

impl From<Topic> for TopicMetaData {
    fn from(value: Topic) -> Self {
        Self::new(value.name, 0, 64)
    }
}
