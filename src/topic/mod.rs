mod creator;
mod reader;
mod writer;

pub use self::creator::{SimpleDiskTopicCreator, TopicCreator};
pub use self::reader::{SimpleDiskTopicReader, TopicReader};
pub use self::writer::{SimpleDiskTopicWriter, TopicWriter};

use std::path::Path;

use crate::types::TopicMetaData;

pub(crate) fn offset_to_file_path(topic_metadata: &TopicMetaData, offset: &usize) -> Box<Path> {
    let num_of_msg_per_file = topic_metadata.num_of_msg_per_file();
    let file_number = offset / num_of_msg_per_file;

    let file_dir = format!("{}", (file_number % topic_metadata.num_of_segments()));
    let file_name = format!("{}.txt", file_number * num_of_msg_per_file);

    topic_metadata
        .topic()
        .path()
        .join(&file_dir)
        .join(&file_name)
        .into_boxed_path()
}

#[allow(dead_code)] // used in testcases
struct TempTopicCreator<T: AsRef<Path>>(T);

#[allow(dead_code)] // used in testcases
impl<T: AsRef<Path>> TempTopicCreator<T> {
    async fn new(root_path: T, topic_metadata: TopicMetaData) -> Self {
        let simple_disk_topic_creator = SimpleDiskTopicCreator::new();
        simple_disk_topic_creator
            .create_topic(topic_metadata.clone())
            .await
            .unwrap();

        Self(root_path)
    }
}

impl<T: AsRef<Path>> Drop for TempTopicCreator<T> {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0); // ignore result
    }
}
