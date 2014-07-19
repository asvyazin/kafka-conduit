module Kafka.Messages.ApiKey (ApiKey(..), fromApiKey, putApiKey) where

import Kafka.Messages.Utils

import Data.Int
import Data.Serialize.Put

data ApiKey = Produce
            | Fetch
            | Offset
            | Metadata
            | LeaderAndIsr
            | StopReplica
            | OffsetCommit
            | OffsetFetch
            deriving (Eq, Show)

fromApiKey :: ApiKey -> Int16
fromApiKey Produce = 0
fromApiKey Fetch = 1
fromApiKey Offset = 2
fromApiKey Metadata = 3
fromApiKey LeaderAndIsr = 4
fromApiKey StopReplica = 5
fromApiKey OffsetCommit = 8
fromApiKey OffsetFetch = 9

putApiKey :: ApiKey -> Put
putApiKey = putInt16be . fromApiKey
