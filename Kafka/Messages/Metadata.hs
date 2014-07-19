module Kafka.Messages.Metadata (
  Request(..),
  apiVersion,
  apiKey,
  putRequest,
  Response(..),
  Broker(..),
  TopicMetadata(..),
  PartitionMetadata(..),
  getResponse) where

import Kafka.Messages.ApiKey
import Kafka.Messages.Utils

import Control.Applicative
import Data.ByteString
import Data.Int
import Data.Serialize.Get
import Data.Serialize.Put

apiVersion :: Int16
apiVersion = 0

apiKey :: ApiKey
apiKey = Metadata

data Request = Request { topics :: [ByteString] } deriving (Eq, Show)

putRequest :: Request -> Put
putRequest = putArray putString . topics

type NodeId = Int32

data Response = Response { brokers :: [Broker]
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
getPartitionMetadata = PartitionMetadata <$> getErrorCode <*> getInt32be <*> getInt32be <*> getArray getInt32be <*> getArray getInt32be

getTopicMetadata :: Get TopicMetadata
getTopicMetadata = TopicMetadata <$> getErrorCode <*> getString <*> getArray getPartitionMetadata

getBroker :: Get Broker
getBroker = Broker <$> getInt32be <*> getString <*> getInt32be

getResponse :: Get Response
getResponse = Response <$> getArray getBroker <*> getArray getTopicMetadata
