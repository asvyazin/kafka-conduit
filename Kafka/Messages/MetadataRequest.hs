module Kafka.Messages.MetadataRequest where

import Kafka.Messages.Request

import Data.Binary.Put
import Data.ByteString
import Data.Int

currentApiVersion :: Int16
currentApiVersion = 0

data MetadataRequest = MetadataRequest { topics :: [ByteString] } deriving (Eq, Show)

putMetadataRequest :: MetadataRequest -> Put
putMetadataRequest = putArray putString . topics
