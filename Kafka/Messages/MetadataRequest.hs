module Kafka.Messages.MetadataRequest where

import Kafka.Messages.ApiKey
import Kafka.Messages.Request
import Kafka.Messages.Utils

import Data.Serialize.Put
import Data.ByteString
import Data.Int

currentApiVersion :: Int16
currentApiVersion = 0

data MetadataRequest = MetadataRequest { topics :: [ByteString] } deriving (Eq, Show)

putMetadataRequest :: MetadataRequest -> Put
putMetadataRequest = putArray putString . topics

instance IsRequest MetadataRequest where
  getApiKey _ = MetadataRequestApiKey
  getApiVersion _ = 0
  putRequest = putMetadataRequest
