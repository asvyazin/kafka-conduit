module Kafka.Messages.OffsetRequest where

import Kafka.Messages.ApiKey
import Kafka.Messages.Request
import Kafka.Messages.Utils

import Data.ByteString
import Data.Int
import Data.Serialize.Put

data OffsetRequest = OffsetRequest { offsetRequestReplicaId :: Int32
                                   , offsetRequestTopics :: [OffsetRequestTopicItem] }
                   deriving (Eq, Show)

putOffsetRequest :: OffsetRequest -> Put
putOffsetRequest r = putInt32be (offsetRequestReplicaId r) >>
                     putArray putOffsetRequestTopicItem (offsetRequestTopics r)

data OffsetRequestTopicItem = OffsetRequestTopicItem { offsetRequestTopicName :: ByteString
                                                     , offsetRequestPartitions :: [OffsetRequestPartitionItem] }
                            deriving (Eq, Show)

putOffsetRequestTopicItem :: OffsetRequestTopicItem -> Put
putOffsetRequestTopicItem r = putString (offsetRequestTopicName r) >>
                              putArray putOffsetRequestPartitionItem (offsetRequestPartitions r)

data OffsetRequestPartitionItem = OffsetRequestPartitionItem { offsetRequestPartitionId :: Int32
                                                             , offsetRequestTime :: Int64
                                                             , offsetRequestMaxNumberOfOffsets :: Int32 }
                                  deriving (Eq, Show)

putOffsetRequestPartitionItem :: OffsetRequestPartitionItem -> Put
putOffsetRequestPartitionItem r = putInt32be (offsetRequestPartitionId r) >>
                                  putInt64be (offsetRequestTime r) >>
                                  putInt32be (offsetRequestMaxNumberOfOffsets r)

instance IsRequest OffsetRequest where
  getApiKey _ = OffsetRequestApiKey
  getApiVersion _ = 0
  putRequest = putOffsetRequest
