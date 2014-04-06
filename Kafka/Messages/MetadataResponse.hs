module Kafka.Messages.MetadataResponse where

import Kafka.Messages.Utils

import Control.Applicative
import Data.Serialize.Get
import Data.ByteString
import Data.Int

type NodeId = Int32

data MetadataResponse = MetadataResponse { brokers :: [Broker]
                                         , topicsMetadata :: [TopicMetadata] } deriving (Eq, Show)

data Broker = Broker { nodeId :: NodeId
                     , host :: ByteString
                     , port :: Int32 } deriving (Eq, Show)

data TopicMetadata = TopicMetadata { topicErrorCode :: ErrorCode
                                   , topicName :: ByteString
                                   , partitionsMetadata :: [PartitionMetadata] } deriving (Eq, Show)

data PartitionMetadata = PartitionMetadata { partitionErrorCode :: ErrorCode
                                           , partitionId :: Int32
                                           , leader :: NodeId
                                           , replicas :: [NodeId]
                                           , isr :: [NodeId] } deriving (Eq, Show)

getPartitionMetadata :: Get PartitionMetadata
getPartitionMetadata = PartitionMetadata <$> getInt16be <*> getInt32be <*> getInt32be <*> getArray getInt32be <*> getArray getInt32be

getTopicMetadata :: Get TopicMetadata
getTopicMetadata = TopicMetadata <$> getInt16be <*> getString <*> getArray getPartitionMetadata

getBroker :: Get Broker
getBroker = Broker <$> getInt32be <*> getString <*> getInt32be

getMetadataResponse :: Get MetadataResponse
getMetadataResponse = MetadataResponse <$> getArray getBroker <*> getArray getTopicMetadata
