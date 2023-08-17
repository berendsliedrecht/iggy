use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

const MAX_PARTITIONS_COUNT: u32 = 100000;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DeletePartitions {
    #[serde(skip)]
    pub stream_id: Identifier,
    #[serde(skip)]
    pub topic_id: Identifier,
    pub partitions_count: u32,
}

impl CommandPayload for DeletePartitions {}

impl Default for DeletePartitions {
    fn default() -> Self {
        DeletePartitions {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partitions_count: 1,
        }
    }
}

impl Validatable for DeletePartitions {
    fn validate(&self) -> Result<(), Error> {
        if !(1..=MAX_PARTITIONS_COUNT).contains(&self.partitions_count) {
            return Err(Error::TooManyPartitions);
        }

        Ok(())
    }
}

impl FromStr for DeletePartitions {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 3 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let topic_id = parts[1].parse::<Identifier>()?;
        let partitions_count = parts[2].parse::<u32>()?;
        let command = DeletePartitions {
            stream_id,
            topic_id,
            partitions_count,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for DeletePartitions {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(4 + stream_id_bytes.len() + topic_id_bytes.len());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(self.partitions_count.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<DeletePartitions, Error> {
        if bytes.len() < 10 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes)?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        position += topic_id.get_size_bytes() as usize;
        let partitions_count = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let command = DeletePartitions {
            stream_id,
            topic_id,
            partitions_count,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for DeletePartitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}",
            self.stream_id, self.topic_id, self.partitions_count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = DeletePartitions {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            partitions_count: 3,
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(&bytes).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let partitions_count =
            u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partitions_count, command.partitions_count);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let partitions_count = 3u32;
        let stream_id_bytes = stream_id.as_bytes();
        let topic_id_bytes = topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(4 + stream_id_bytes.len() + topic_id_bytes.len());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(partitions_count.to_le_bytes());
        let command = DeletePartitions::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partitions_count, partitions_count);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let partitions_count = 3u32;
        let input = format!("{}|{}|{}", stream_id, topic_id, partitions_count);
        let command = DeletePartitions::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partitions_count, partitions_count);
    }
}