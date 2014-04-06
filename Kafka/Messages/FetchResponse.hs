module Kafka.Messages.FetchResponse where

import Kafka.Messages.MessageSet
import Kafka.Messages.Utils

import Control.Applicative
import Data.ByteString
import Data.Int
import Data.Serialize.Get

data FetchResponse = FetchResponse { fetchResponseTopics :: [FetchResponseTopicItem] } deriving (Eq, Show)

getFetchResponse :: Get FetchResponse
getFetchResponse = FetchResponse <$> getArray getFetchResponseTopicItem

data FetchResponseTopicItem = FetchResponseTopicItem { fetchResponseTopicName :: ByteString
                                                     , fetchResponsePartitions :: [FetchResponsePartitionItem] }
                            deriving (Eq, Show)

getFetchResponseTopicItem :: Get FetchResponseTopicItem
getFetchResponseTopicItem = FetchResponseTopicItem <$> getString <*> getArray getFetchResponsePartitionItem

data FetchResponsePartitionItem = FetchResponsePartitionItem { fetchResponsePartitionId :: Int32
                                                             , fetchResponseErrorCode :: ErrorCode
                                                             , fetchResponseHighwaterMarkOffset :: Int64
                                                             , fetchResponseMessages :: MessageSet }
                                  deriving (Eq, Show)

getFetchResponsePartitionItem :: Get FetchResponsePartitionItem
getFetchResponsePartitionItem = FetchResponsePartitionItem <$> getInt32be <*> getErrorCode <*> getInt64be <*> getMessageSet
