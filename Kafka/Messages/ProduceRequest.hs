module Kafka.Messages.ProduceRequest where

import Kafka.Messages.MessageSet
import Kafka.Messages.Utils

import Data.ByteString
import Data.Int
import Data.Serialize.Put

data ProduceRequest = ProduceRequest { produceRequestRequiredAcks :: Int16
                                     , produceRequestTimeout :: Int32
                                     , produceRequestTopics :: [ProduceRequestTopic] } deriving (Eq, Show)

putProduceRequest :: ProduceRequest -> Put
putProduceRequest r = putInt16be (produceRequestRequiredAcks r) >>
                      putInt32be (produceRequestTimeout r) >>
                      putArray putProduceRequestTopic (produceRequestTopics r)

data ProduceRequestTopic = ProduceRequestTopic { produceRequestTopicName :: ByteString
                                               , produceRequestPartitions :: [ProduceRequestPartition] } deriving (Eq, Show)

putProduceRequestTopic :: ProduceRequestTopic -> Put
putProduceRequestTopic r = putString (produceRequestTopicName r) >>
                           putArray putProduceRequestPartition (produceRequestPartitions r)

data ProduceRequestPartition = ProduceRequestPartition { produceRequestPartitionId :: Int32
                                                       , produceRequestMessages :: MessageSet } deriving (Eq, Show)

putProduceRequestPartition :: ProduceRequestPartition -> Put
putProduceRequestPartition r = putInt32be (produceRequestPartitionId r) >>
                               putMessageSet (produceRequestMessages r)
