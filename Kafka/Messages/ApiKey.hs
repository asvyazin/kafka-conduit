module Kafka.Messages.ApiKey where

import Kafka.Messages.Utils

import Data.Int
import Data.Serialize.Put

data ApiKey = ProduceRequestApiKey
            | FetchRequestApiKey
            | OffsetRequestApiKey
            | MetadataRequestApiKey
            | LeaderAndIsrRequestApiKey
            | StopReplicaRequestApiKey
            | OffsetCommitRequestApiKey
            | OffsetFetchRequestApiKey
            deriving (Eq, Show)

fromApiKey :: ApiKey -> Int16
fromApiKey ProduceRequestApiKey = 0
fromApiKey FetchRequestApiKey = 1
fromApiKey OffsetRequestApiKey = 2
fromApiKey MetadataRequestApiKey = 3
fromApiKey LeaderAndIsrRequestApiKey = 4
fromApiKey StopReplicaRequestApiKey = 5
fromApiKey OffsetCommitRequestApiKey = 8
fromApiKey OffsetFetchRequestApiKey = 9

putApiKey :: ApiKey -> Put
putApiKey = putInt16be . fromApiKey
