use std::error::Error;
use std::path::Path;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use crate::types::{Message, TopicMetaData};

#[async_trait]
pub trait TopicWriter {
    type Error;

    async fn write(&mut self, msg: Message) -> Result<(), Self::Error>;
}

pub struct SimpleDiskTopicWriter {
    data_insertion_file_path: Box<Path>,
    writer_offset: usize,
    topic_metadata: TopicMetaData,
}

impl SimpleDiskTopicWriter {
    pub async fn new(topic_metadata: TopicMetaData) -> Self {
        let topic_path = topic_metadata.topic().path();

        let writer_offset: usize = tokio::fs::read_to_string(topic_path.join(".writer_offset.txt"))
            .await
            .unwrap_or("0".to_owned())
            .parse()
            .unwrap_or(0);

        let data_insertion_file_path = super::offset_to_file_path(&topic_metadata, &writer_offset);

        Self {
            data_insertion_file_path,
            writer_offset,
            topic_metadata,
        }
    }

    pub async fn flush_topic_metadata(&mut self) -> Result<(), Box<dyn Error>> {
        let mut file = tokio::fs::File::create(self.topic_metadata.topic().metadata_path()).await?;
        
        unsafe {
            let new_offset = if self.writer_offset == 0 { None } else { Some(self.writer_offset - 1) };
            self.topic_metadata.set_last_flushed_offset(new_offset);
        }

        let metadata = toml::to_string(&self.topic_metadata)?;
        file.write_all(metadata.as_bytes()).await?;

        Ok(())
    }
}

#[async_trait]
impl TopicWriter for SimpleDiskTopicWriter {
    type Error = std::io::Error;

    /// appends msg.value to the end of approperiate file
    /// assumes msg.value contains \n at the end of msg
    /// data (value).
    async fn write(&mut self, msg: Message) -> Result<(), Self::Error> {
        let mut file = match tokio::fs::File::options()
            .append(true)
            .open(&self.data_insertion_file_path)
            .await
        {
            Ok(file) => file,
            Err(e) if e.kind() == tokio::io::ErrorKind::NotFound => {
                tokio::fs::File::create(&self.data_insertion_file_path).await?
            }
            Err(e) => return Err(e),
        };

        file.write_all(msg.value()).await?;

        self.writer_offset += 1;

        if self.writer_offset % self.topic_metadata.num_of_msg_per_file() == 0 {
            self.data_insertion_file_path =
                super::offset_to_file_path(&self.topic_metadata, &self.writer_offset);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::{offset_to_file_path, TempTopicCreator};
    use crate::types::{Topic, TopicMetaData};
    use bytes::Bytes;
    use tokio::test;

    #[test]
    async fn simple_disk_topic_writer_dot_flush_topic_metadata_test_01() {
        let root_path = "./simple_disk_topic_writer_dot_flush_topic_metadata_test_01";

        let topic = Topic::new("foo".to_owned(), root_path);
        let topic_metadata = TopicMetaData::new_with_few_defaults(topic);

        let _temp_topic = TempTopicCreator::new(root_path, topic_metadata.clone()).await;

        let mut simple_disk_topic_writer = SimpleDiskTopicWriter::new(topic_metadata.clone()).await;
        simple_disk_topic_writer
            .flush_topic_metadata()
            .await
            .unwrap();

        let written_metadata = tokio::fs::read_to_string(topic_metadata.topic().metadata_path())
            .await
            .unwrap();
        let written_metadata: TopicMetaData = toml::from_str(&written_metadata).unwrap();

        assert_eq!(topic_metadata, written_metadata);
    }

    #[test]
    async fn simple_disk_topic_writer_dot_write_test_produce_single_msg_01() {
        let root_path = "./simple_disk_topic_writer_dot_write_test_produce_single_msg_01";

        let topic = Topic::new("foo".to_owned(), root_path);
        let topic_metadata = TopicMetaData::new_with_few_defaults(topic);

        let _temp_topic = TempTopicCreator::new(root_path, topic_metadata.clone()).await;

        let mut simple_disk_topic_writer = SimpleDiskTopicWriter::new(topic_metadata.clone()).await;

        let msg_to_be_written = "hello\n";
        let msg = Message::new(Bytes::from(msg_to_be_written), None);
        simple_disk_topic_writer.write(msg).await.unwrap();

        let written_path = offset_to_file_path(&topic_metadata, &0);
        let written_msg = tokio::fs::read_to_string(written_path).await.unwrap();

        assert_eq!(written_msg.as_str(), msg_to_be_written);
    }

    #[test]
    async fn simple_disk_topic_writer_dot_write_test_produce_multi_msg_01() {
        let root_path = "./simple_disk_topic_writer_dot_write_test_produce_multi_msg_01";

        let topic = Topic::new("foo".to_owned(), root_path);
        let topic_metadata = TopicMetaData::new_with_few_defaults(topic);

        let _temp_topic = TempTopicCreator::new(root_path, topic_metadata.clone()).await;

        let mut simple_disk_topic_writer = SimpleDiskTopicWriter::new(topic_metadata.clone()).await;

        let mut written_msg_content_should_be = String::new();
        for i in 0..(*topic_metadata.num_of_msg_per_file()) {
            let msg_val = format!("hello{i}\n");

            written_msg_content_should_be.push_str(&msg_val[..]);

            let msg = Message::new(Bytes::from(msg_val), None);
            simple_disk_topic_writer.write(msg).await.unwrap();
        }

        let written_path = offset_to_file_path(&topic_metadata, &0);
        let written_msg = tokio::fs::read_to_string(written_path).await.unwrap();

        assert_eq!(written_msg, written_msg_content_should_be);
    }

    #[test]
    async fn simple_disk_topic_writer_dot_write_test_produce_multi_msg_02() {
        let root_path = "./simple_disk_topic_writer_dot_write_test_produce_multi_msg_02";

        let topic = Topic::new("foo".to_owned(), root_path);
        let topic_metadata = TopicMetaData::new_with_few_defaults(topic);

        let _temp_topic = TempTopicCreator::new(root_path, topic_metadata.clone()).await;

        let mut simple_disk_topic_writer = SimpleDiskTopicWriter::new(topic_metadata.clone()).await;

        let mut written_msg_content_should_be = String::new();
        for i in 0..(*topic_metadata.num_of_msg_per_file()) {
            let msg_val = format!("hello{i}\n");

            written_msg_content_should_be.push_str(&msg_val[..]);

            let msg = Message::new(Bytes::from(msg_val), None);
            simple_disk_topic_writer.write(msg).await.unwrap();
        }

        let written_path = offset_to_file_path(&topic_metadata, &0);
        let written_msg = tokio::fs::read_to_string(written_path).await.unwrap();

        assert_eq!(written_msg, written_msg_content_should_be);

        let msg_val = "this_msg_should_go_in_new_file\n";
        let msg = Message::new(Bytes::from(msg_val), None);
        simple_disk_topic_writer.write(msg).await.unwrap();

        let written_path = offset_to_file_path(&topic_metadata, &(topic_metadata.num_of_msg_per_file() + 1));
        let written_msg = tokio::fs::read_to_string(written_path).await.unwrap();

        assert_eq!(written_msg.as_str(), msg_val);
    }

}
