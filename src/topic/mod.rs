#![allow(unused_variables)]

use std::error::Error;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::DirBuilder;
use tokio::io::AsyncWriteExt;

use crate::types::TopicMetaData;

#[async_trait(?Send)]
pub trait TopicCreator {
    type Error;

    async fn create_topic(&self, metadata: TopicMetaData) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait TopicWriter {
    type Error;

    async fn publish_msg(&self, msg: Bytes) -> Result<(), Self::Error>;
}

pub struct SimpleDiskTopicCreator {}

pub struct SimpleDiskTopicWriter {}

impl SimpleDiskTopicCreator {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait(?Send)]
impl TopicCreator for SimpleDiskTopicCreator {
    type Error = Box<dyn Error>;

    async fn create_topic(&self, metadata: TopicMetaData) -> Result<(), Self::Error> {
        let mut builder = DirBuilder::new();
        let builder = builder.recursive(true);

        let topic_path = metadata.topic().path();

        builder.create(topic_path).await?;

        let mut file = tokio::fs::File::create(metadata.topic().metadata_path()).await?;
        let topic_metadata_toml = toml::to_string(&metadata)?;

        file.write_all(topic_metadata_toml.as_bytes()).await?;

        let num_of_segments = *metadata.num_of_segments();
        for i in 0..num_of_segments {
            let dir_path = topic_path.join(format!("{i}"));
            builder.create(dir_path).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl TopicWriter for SimpleDiskTopicWriter {
    type Error = Box<dyn Error>;

    async fn publish_msg(&self, msg: Bytes) -> Result<(), Self::Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Topic, TopicMetaData};
    use tokio::test;

    #[test]
    async fn simple_disk_topic_creator_test_01() {
        let root_path = "./simple_disk_topic_creator_test_01";

        let topic = Topic::new("foo".to_owned(), root_path);
        let topic_metadata = TopicMetaData::new_with_few_defaults(topic);

        let simple_disk_topic_creator = SimpleDiskTopicCreator::new();

        simple_disk_topic_creator
            .create_topic(topic_metadata)
            .await
            .unwrap();

        tokio::fs::remove_dir_all(root_path).await.unwrap();
    }
}
