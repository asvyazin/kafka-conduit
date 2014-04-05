module Kafka.Messages.ProduceResponse where

import Kafka.Messages.Response
import Kafka.Messages.Utils

import Control.Applicative
import Data.ByteString
import Data.Int
import Data.Serialize.Get

data ProduceResponse = ProduceResponse [ProduceResponseTopic] deriving (Eq, Show)

getProduceResponse :: Get ProduceResponse
getProduceResponse = ProduceResponse <$> getArray getProduceResponseTopic

data ProduceResponseTopic = ProduceResponseTopic { produceResponseTopicName :: ByteString
                                                 , produceResponsePartitions :: [ProduceResponsePartition] } deriving (Eq, Show)

getProduceResponseTopic :: Get ProduceResponseTopic
getProduceResponseTopic = ProduceResponseTopic <$> getString <*> getArray getProduceResponsePartition

data ProduceResponsePartition = ProduceResponsePartition { produceResponsePartitionId :: Int32
                                                         , produceResponsePartitionErrorCode :: ErrorCode
                                                         , produceResponsePartitionOffset :: Int64 } deriving (Eq, Show)

getProduceResponsePartition :: Get ProduceResponsePartition
getProduceResponsePartition = ProduceResponsePartition <$> getInt32be <*> getInt16be <*> getInt64be
