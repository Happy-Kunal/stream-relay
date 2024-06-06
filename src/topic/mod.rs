#![allow(unused_variables)]

use std::io::Result;
use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::DirBuilder;
use tokio::io::AsyncWriteExt;

use crate::types::{Topic, TopicMetaData};

#[async_trait(?Send)]
pub trait TopicCreator {
    async fn create_topic(&self, topic: Topic) -> Result<()>;
}

#[async_trait]
pub trait TopicWriter {
    async fn publish_msg(&self, msg: Bytes) -> Result<()>;
}

pub struct SimpleDiskTopicCreator<T>
where
    T: AsRef<Path>,
{
    root_path: T,
}

pub struct SimpleDiskTopicWriter {}

impl<T> SimpleDiskTopicCreator<T>
where
    T: AsRef<Path>,
{
    pub fn new(root_path: T) -> Self {
        Self { root_path }
    }
}

#[async_trait(?Send)]
impl<T> TopicCreator for SimpleDiskTopicCreator<T>
where
    T: AsRef<Path>,
{
    async fn create_topic(&self, topic: Topic) -> Result<()> {
        let mut builder = DirBuilder::new();
        let builder = builder.recursive(true);

        let topic_path = self.root_path.as_ref().join(topic.name());
        builder.create(topic_path.clone()).await?;

        let metadata = TopicMetaData::from(topic);
        let mut file = tokio::fs::File::create(topic_path.join("metadata.toml")).await?;
        let topic_metadata_toml = toml::to_string(&metadata).unwrap();

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
    async fn publish_msg(&self, msg: Bytes) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;

    #[test]
    async fn simple_disk_topic_creator_test_01() {
        let root_path = "./simple_disk_topic_creator_test_01";
        let topic = Topic::new("foo".to_owned());
        let simple_disk_topic_creator = SimpleDiskTopicCreator::new(root_path);

        simple_disk_topic_creator.create_topic(topic).await.unwrap();

        tokio::fs::remove_dir_all(root_path).await.unwrap();
    }
}
