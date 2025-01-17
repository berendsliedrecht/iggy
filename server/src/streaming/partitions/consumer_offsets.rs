use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::polling_consumer::PollingConsumer;
use iggy::consumer::ConsumerKind;
use iggy::error::Error;
use std::collections::HashMap;
use tracing::trace;

impl Partition {
    pub async fn get_consumer_offset(&self, consumer: PollingConsumer) -> Result<u64, Error> {
        trace!(
            "Getting consumer offset for {}, partition: {}, current: {}...",
            consumer,
            self.partition_id,
            self.current_offset
        );

        match consumer {
            PollingConsumer::Consumer(consumer_id, _) => {
                let consumer_offsets = self.consumer_offsets.read().await;
                let consumer_offset = consumer_offsets.get(&consumer_id);
                if let Some(consumer_offset) = consumer_offset {
                    return Ok(consumer_offset.offset);
                }
            }
            PollingConsumer::ConsumerGroup(consumer_group_id, _) => {
                let consumer_offsets = self.consumer_group_offsets.read().await;
                let consumer_offset = consumer_offsets.get(&consumer_group_id);
                if let Some(consumer_offset) = consumer_offset {
                    return Ok(consumer_offset.offset);
                }
            }
        }

        Ok(0)
    }

    pub async fn store_consumer_offset(
        &self,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), Error> {
        trace!(
            "Storing offset: {} for {}, partition: {}, current: {}...",
            offset,
            consumer,
            self.partition_id,
            self.current_offset
        );
        if offset > self.current_offset {
            return Err(Error::InvalidOffset(offset));
        }

        match consumer {
            PollingConsumer::Consumer(consumer_id, _) => {
                let mut consumer_offsets = self.consumer_offsets.write().await;
                self.store_offset(
                    ConsumerKind::Consumer,
                    consumer_id,
                    offset,
                    &mut consumer_offsets,
                )
                .await?;
            }
            PollingConsumer::ConsumerGroup(consumer_id, _) => {
                let mut consumer_offsets = self.consumer_group_offsets.write().await;
                self.store_offset(
                    ConsumerKind::ConsumerGroup,
                    consumer_id,
                    offset,
                    &mut consumer_offsets,
                )
                .await?;
            }
        };

        Ok(())
    }

    async fn store_offset(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: u64,
        consumer_offsets: &mut HashMap<u32, ConsumerOffset>,
    ) -> Result<(), Error> {
        if let Some(consumer_offset) = consumer_offsets.get_mut(&consumer_id) {
            consumer_offset.offset = offset;
            self.storage
                .partition
                .save_consumer_offset(consumer_offset)
                .await?;
            return Ok(());
        }

        let consumer_offset = ConsumerOffset::new(
            kind,
            consumer_id,
            offset,
            self.stream_id,
            self.topic_id,
            self.partition_id,
        );
        self.storage
            .partition
            .save_consumer_offset(&consumer_offset)
            .await?;
        consumer_offsets.insert(consumer_id, consumer_offset);
        Ok(())
    }

    pub async fn load_consumer_offsets(&mut self) -> Result<(), Error> {
        trace!(
                "Loading consumer offsets for partition with ID: {} for topic with ID: {} and stream with ID: {}...",
                self.partition_id,
                self.topic_id,
                self.stream_id
            );
        self.load_consumer_offsets_from_storage(ConsumerKind::Consumer)
            .await?;
        self.load_consumer_offsets_from_storage(ConsumerKind::ConsumerGroup)
            .await
    }

    async fn load_consumer_offsets_from_storage(&self, kind: ConsumerKind) -> Result<(), Error> {
        let loaded_consumer_offsets = self
            .storage
            .partition
            .load_consumer_offsets(kind, self.stream_id, self.topic_id, self.partition_id)
            .await?;
        let mut consumer_offsets = match kind {
            ConsumerKind::Consumer => self.consumer_offsets.write().await,
            ConsumerKind::ConsumerGroup => self.consumer_group_offsets.write().await,
        };
        for consumer_offset in loaded_consumer_offsets {
            self.log_consumer_offset(&consumer_offset);
            consumer_offsets.insert(consumer_offset.consumer_id, consumer_offset);
        }
        Ok(())
    }

    fn log_consumer_offset(&self, consumer_offset: &ConsumerOffset) {
        trace!("Loaded consumer offset value: {} for {} with ID: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                consumer_offset.offset,
                consumer_offset.kind,
                consumer_offset.consumer_id,
                self.partition_id,
                self.topic_id,
                self.stream_id
            );
    }
}
