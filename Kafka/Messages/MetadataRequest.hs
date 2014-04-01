module Kafka.Messages.MetadataRequest where

import Kafka.Messages.Request

import Data.Binary.Put
import Data.ByteString
import Data.ByteString.Lazy (toStrict)
import Data.Int

currentApiVersion :: Int16
currentApiVersion = 0

data MetadataRequest = MetadataRequest { topics :: [ByteString] } deriving (Eq, Show)

putMetadataRequest :: MetadataRequest -> Put
putMetadataRequest = putArray putString . topics

instance RequestMessage MetadataRequest where
  toRawRequest corrId cliId r = RawRequest MetadataRequestApiKey currentApiVersion corrId cliId $ toStrict $ runPut $ putMetadataRequest r
