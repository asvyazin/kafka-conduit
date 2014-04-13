module Kafka.Messages.FetchRequest where

import Kafka.Messages.ApiKey
import Kafka.Messages.Request
import Kafka.Messages.Utils

import Data.ByteString
import Data.Int
import Data.Serialize.Put

data FetchRequest = FetchRequest { fetchRequestReplicaId :: Int32
                                 , fetchRequestMaxWaitTime :: Int32
                                 , fetchRequestMinBytes :: Int32
                                 , fetchRequestTopics :: [FetchRequestTopicItem] }
                  deriving (Eq, Show)

putFetchRequest :: FetchRequest -> Put
putFetchRequest r = putInt32be (fetchRequestReplicaId r) >>
                    putInt32be (fetchRequestMaxWaitTime r) >>
                    putInt32be (fetchRequestMinBytes r) >>
                    putArray putFetchRequestTopicItem (fetchRequestTopics r)

data FetchRequestTopicItem = FetchRequestTopicItem { fetchRequestTopicName :: ByteString
                                                   , fetchRequestPartitions :: [FetchRequestPartitionItem] }
                             deriving (Eq, Show)

putFetchRequestTopicItem :: FetchRequestTopicItem -> Put
putFetchRequestTopicItem r = putString (fetchRequestTopicName r) >>
                             putArray putFetchRequestPartitionItem (fetchRequestPartitions r)

data FetchRequestPartitionItem = FetchRequestPartitionItem { fetchRequestPartitionId :: Int32
                                                           , fetchRequestFetchOffset :: Int64
                                                           , fetchRequestMaxBytes :: Int32 }
                               deriving (Eq, Show)

putFetchRequestPartitionItem :: FetchRequestPartitionItem -> Put
putFetchRequestPartitionItem r = putInt32be (fetchRequestPartitionId r) >>
                                 putInt64be (fetchRequestFetchOffset r) >>
                                 putInt32be (fetchRequestMaxBytes r)

instance IsRequest FetchRequest where
  getApiKey _ = FetchRequestApiKey
  getApiVersion _ = 0
  putRequest = putFetchRequest
