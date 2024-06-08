use std::{
    error::Error,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncBufReadExt;

use crate::types::{Message, TopicMetaData};

#[async_trait]
pub trait TopicReader {
    type Error;

    async fn read(&mut self, offset: usize) -> Result<Option<Message>, Self::Error>;
}

pub struct SimpleDiskTopicReader {
    topic_metadata: TopicMetaData,
    last_updated: SystemTime,
    update_interval: Duration,
    min_time_for_next_update: SystemTime, // stored here as well to increase reader perf
    last_flushed_offset: Option<usize>,           // stored here as well to increase reader perf
}

pub struct CachedDiskTopicReader {}

impl SimpleDiskTopicReader {
    pub fn new(topic_metadata: TopicMetaData, last_updated: SystemTime, update_interval: Duration) -> Self {
        let last_flushed_offset = *topic_metadata.last_flushed_offset();
        Self {
            topic_metadata,
            last_updated,
            update_interval,
            min_time_for_next_update: last_updated + update_interval,
            last_flushed_offset,
        }
    }

    pub async fn update_topics_metadata_info_if_changed(&mut self) -> Result<(), Box<dyn Error>> {
        let metadata_path = self.topic_metadata.topic().metadata_path();
        let last_metadata_modification_time = tokio::fs::metadata(metadata_path)
            .await?
            .modified()
            .expect("system doesn't store file last modification time in file metadata");

        if last_metadata_modification_time >= self.last_updated {
            let latest_metadata = tokio::fs::read_to_string(metadata_path).await?;
            let latest_metadata: TopicMetaData = toml::from_str(&latest_metadata)?;

            self.topic_metadata = latest_metadata;
            self.last_updated = last_metadata_modification_time;
            self.min_time_for_next_update = last_metadata_modification_time + self.update_interval;
            self.last_flushed_offset = *self.topic_metadata.last_flushed_offset();
        }

        Ok(())
    }
}

#[async_trait]
impl TopicReader for SimpleDiskTopicReader {
    type Error = Box<dyn Error>;

    async fn read(&mut self, offset: usize) -> Result<Option<Message>, Self::Error> {
        if self.last_flushed_offset.is_none() || offset > self.last_flushed_offset.unwrap() && self.min_time_for_next_update < SystemTime::now() {
            let _result = self.update_topics_metadata_info_if_changed().await; // ignore result
        }

        if self.last_flushed_offset.is_none() || offset > self.last_flushed_offset.unwrap() {
            Ok(None)
        } else {
            let filepath = super::offset_to_file_path(&self.topic_metadata, &offset);
            let line_number = offset % self.topic_metadata.num_of_msg_per_file();

            let file = tokio::fs::File::open(filepath).await?;
            let mut reader = tokio::io::BufReader::new(file);

            let mut line = String::new();
            for _ in 0..line_number {
                let _ = reader.read_line(&mut line).await; // ignore result
                line.clear();
            }

            let _ = reader.read_line(&mut line).await; // ignore result

            let msg = Message::new(Bytes::from(line), Some(offset));

            Ok(Some(msg))
        }
    }
}

impl CachedDiskTopicReader {}

#[async_trait]
impl TopicReader for CachedDiskTopicReader {
    type Error = Box<dyn Error>;

    async fn read(&mut self, _offset: usize) -> Result<Option<Message>, Self::Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Topic, TopicMetaData};
    use tokio::test;
    use crate::topic::{TempTopicCreator, SimpleDiskTopicWriter, TopicWriter};

    async fn simple_disk_topic_reader_test_boiler_plate(root_path: &str, topic_metadata: TopicMetaData, num_of_msgs: usize) {
        let _temp_topic = TempTopicCreator::new(root_path, topic_metadata.clone()).await;

        let mut simple_disk_topic_writer = SimpleDiskTopicWriter::new(topic_metadata.clone()).await;
        let mut simple_disk_topic_reader = SimpleDiskTopicReader::new(topic_metadata.clone(), SystemTime::now(), Duration::from_nanos(0));

        let mut written_msg = vec![];

        for i in 0..num_of_msgs {
            let write_msg = format!("hello{i}\n");
            let write_msg = Message::new(Bytes::from(write_msg), None);
            simple_disk_topic_writer.write(write_msg.clone()).await.unwrap();
            written_msg.push(write_msg);
        }


        tokio::time::sleep(Duration::from_millis(10)).await; // inorder to account for non-monotonic nature of `std::time::SystemTime``

        simple_disk_topic_writer.flush_topic_metadata().await.unwrap();

        for i in 0..num_of_msgs {
            let read_msg = simple_disk_topic_reader.read(i).await.unwrap();
            assert_eq!(written_msg.get(i).unwrap().value(), read_msg.unwrap().value());
        }
    }

    #[test]
    async fn simple_disk_topic_reader_test_01() {
        let root_path = "./simple_disk_topic_reader_test_01";

        let topic = Topic::new("foo".to_owned(), root_path);
        let topic_metadata = TopicMetaData::new(topic, 3, None, 4);

        simple_disk_topic_reader_test_boiler_plate(root_path, topic_metadata, 1).await;
    }

    #[test]
    async fn simple_disk_topic_reader_test_02() {
        let root_path = "./simple_disk_topic_reader_test_02";

        let topic = Topic::new("foo".to_owned(), root_path);
        let topic_metadata = TopicMetaData::new(topic, 3, None, 4);

        simple_disk_topic_reader_test_boiler_plate(root_path, topic_metadata, 2).await;
    }

    #[test]
    async fn simple_disk_topic_reader_test_03() {
        let root_path = "./simple_disk_topic_reader_test_03";

        let topic = Topic::new("foo".to_owned(), root_path);
        let topic_metadata = TopicMetaData::new(topic, 3, None, 4);

        simple_disk_topic_reader_test_boiler_plate(root_path, topic_metadata, 3).await;
    }

    #[test]
    async fn simple_disk_topic_reader_test_04() {
        let root_path = "./simple_disk_topic_reader_test_04";

        let topic = Topic::new("foo".to_owned(), root_path);
        let topic_metadata = TopicMetaData::new(topic, 3, None, 4);

        simple_disk_topic_reader_test_boiler_plate(root_path, topic_metadata, 3).await;
    }
}
