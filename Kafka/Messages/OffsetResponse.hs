module Kafka.Messages.OffsetResponse where

import Kafka.Messages.Utils

import Control.Applicative
import Data.ByteString
import Data.Int
import Data.Serialize.Get

data OffsetResponse = OffsetResponse { offsetResponseTopics :: [OffsetResponseTopicItem] } deriving (Eq, Show)

getOffsetResponse :: Get OffsetResponse
getOffsetResponse = OffsetResponse <$> getArray getOffsetResponseTopicItem

data OffsetResponseTopicItem = OffsetResponseTopicItem { offsetResponseTopicName :: ByteString
                                                       , offsetResponsePartitions :: [OffsetResponsePartitionItem] }
                             deriving (Eq, Show)

getOffsetResponseTopicItem :: Get OffsetResponseTopicItem
getOffsetResponseTopicItem = OffsetResponseTopicItem <$> getString <*> getArray getOffsetResponsePartitionItem

data OffsetResponsePartitionItem = OffsetResponsePartitionItem { offsetResponsePartitionId :: Int32
                                                               , offsetResponseOffset :: Int64
                                                               , offsetResponseMetadata :: ByteString }
                                 deriving (Eq, Show)

getOffsetResponsePartitionItem :: Get OffsetResponsePartitionItem
getOffsetResponsePartitionItem = OffsetResponsePartitionItem <$> getInt32be <*> getInt64be <*> getString
