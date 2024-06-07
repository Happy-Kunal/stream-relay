use std::path::Path;

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
    path: Box<Path>,
    metadata_path: Box<Path>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TopicWithOffset {
    topic: Topic,
    offset: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TopicMetaData {
    topic: Topic,
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
    pub fn new<T: AsRef<Path>>(name: String, root_path: T) -> Self {
        let path = root_path.as_ref().join(&name).into_boxed_path();
        let metadata_path = path.join("metadata.toml").into_boxed_path();

        Self {
            name,
            path,
            metadata_path,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> &Path {
        self.path.as_ref()
    }

    pub fn metadata_path(&self) -> &Path {
        self.metadata_path.as_ref()
    }
}

impl TopicWithOffset {
    pub fn new(topic: Topic, offset: usize) -> Self {
        Self { topic, offset }
    }

    pub fn offset(&self) -> &usize {
        &self.offset
    }
}

impl TopicMetaData {
    pub fn new(topic: Topic, last_flushed_offset: usize, num_of_segments: usize) -> Self {
        Self {
            topic,
            last_flushed_offset,
            num_of_segments,
        }
    }

    pub fn new_with_few_defaults(topic: Topic) -> Self {
        Self {
            topic,
            last_flushed_offset: 0,
            num_of_segments: 64,
        }
    }

    pub fn topic(&self) -> &Topic {
        &self.topic
    }

    pub fn last_flushed_offset(&self) -> &usize {
        &self.last_flushed_offset
    }

    pub fn num_of_segments(&self) -> &usize {
        &self.num_of_segments
    }
}

impl TryFrom<Topic> for TopicMetaData {
    type Error = Box<dyn std::error::Error>;

    /// [BLOCKING I/O Used] tries to read metadata about topic from disk
    /// and deserialize it into `Self`
    /// 
    /// # Error:
    /// if topic doesn't exist on disk OR invalid data in topic's metadata
    /// file to be deserialized.

    fn try_from(value: Topic) -> Result<Self, Self::Error> {
        let metadata = std::fs::read_to_string(value.metadata_path())?;
        let metadata: TopicMetaData = toml::from_str(&metadata)?;

        Ok(metadata)
    }
}
