use crate::common::{ClientFactory, TestServer};
use bytes::Bytes;
use iggy::client::{MessageClient, StreamClient, TopicClient};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingStrategy};
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy::streams::create_stream::CreateStream;
use iggy::topics::create_topic::CreateTopic;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::time::sleep;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const MESSAGES_COUNT: u32 = 1000;
const PARTITION_ID: u32 = 1;

#[allow(dead_code)]
pub async fn run(client_factory: &dyn ClientFactory) {
    let mut test_server = TestServer::default();
    test_server.start();
    sleep(std::time::Duration::from_secs(1)).await;
    let client = client_factory.create_client().await;
    let client = IggyClient::new(client, IggyClientConfig::default(), None, None);
    init_system(&client).await;

    // 1. Send messages with the included headers
    let mut messages = Vec::new();
    for offset in 0..MESSAGES_COUNT {
        let id = (offset + 1) as u128;
        let payload = get_message_payload(offset as u64);
        let headers = get_message_headers();
        messages.push(Message {
            id,
            length: payload.len() as u32,
            payload,
            headers: Some(headers),
        });
    }

    let mut send_messages = SendMessages {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        partitioning: Partitioning::partition_id(PARTITION_ID),
        messages,
    };
    client.send_messages(&mut send_messages).await.unwrap();

    // 2. Poll messages and validate the headers
    let poll_messages = PollMessages {
        consumer: Consumer::default(),
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        partition_id: Some(PARTITION_ID),
        strategy: PollingStrategy::offset(0),
        count: MESSAGES_COUNT,
        auto_commit: false,
    };

    let polled_messages = client.poll_messages(&poll_messages).await.unwrap();
    assert_eq!(polled_messages.messages.len() as u32, MESSAGES_COUNT);
    for i in 0..MESSAGES_COUNT {
        let message = polled_messages.messages.get(i as usize).unwrap();
        assert!(message.headers.is_some());
        let headers = message.headers.as_ref().unwrap();
        assert_eq!(headers.len(), 3);
        assert_eq!(
            headers
                .get(&HeaderKey::new("key_1").unwrap())
                .unwrap()
                .as_str()
                .unwrap(),
            "Value 1"
        );
        assert!(headers
            .get(&HeaderKey::new("key 2").unwrap())
            .unwrap()
            .as_bool()
            .unwrap(),);
        assert_eq!(
            headers
                .get(&HeaderKey::new("key-3").unwrap())
                .unwrap()
                .as_uint64()
                .unwrap(),
            123456
        );
    }

    test_server.stop();
}

async fn init_system(client: &IggyClient) {
    // 1. Create the stream
    let create_stream = CreateStream {
        stream_id: STREAM_ID,
        name: STREAM_NAME.to_string(),
    };
    client.create_stream(&create_stream).await.unwrap();

    // 2. Create the topic
    let create_topic = CreateTopic {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: TOPIC_ID,
        partitions_count: PARTITIONS_COUNT,
        name: TOPIC_NAME.to_string(),
        message_expiry: None,
    };
    client.create_topic(&create_topic).await.unwrap();
}

fn get_message_payload(offset: u64) -> Bytes {
    Bytes::from(format!("message {}", offset))
}

fn get_message_headers() -> HashMap<HeaderKey, HeaderValue> {
    let mut headers = HashMap::new();
    headers.insert(
        HeaderKey::new("key_1").unwrap(),
        HeaderValue::from_str("Value 1").unwrap(),
    );
    headers.insert(
        HeaderKey::new("key 2").unwrap(),
        HeaderValue::from_bool(true).unwrap(),
    );
    headers.insert(
        HeaderKey::new("key-3").unwrap(),
        HeaderValue::from_uint64(123456).unwrap(),
    );
    headers
}