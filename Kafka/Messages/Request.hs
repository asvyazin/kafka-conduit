module Kafka.Messages.Request where

import Kafka.Messages.ApiKey
import Kafka.Messages.Utils

import Data.Serialize.Put
import qualified Data.ByteString as B
import Data.Int

data RawRequest = RawRequest { apiKey :: ApiKey
                             , apiVersion :: Int16
                             , correlationId :: Int32
                             , clientId :: B.ByteString
                             , requestMessageBytes :: B.ByteString } deriving (Eq, Show)

putRawRequest :: RawRequest -> Put
putRawRequest r = putApiKey (apiKey r)
                  >> putInt16be (apiVersion r)
                  >> putInt32be (correlationId r)
                  >> putString (clientId r)
                  >> putByteString (requestMessageBytes r)

serializeRawRequest :: RawRequest -> B.ByteString
serializeRawRequest r = let body = runPut $ putRawRequest r
                            l = B.length body
                        in runPut (putWord32be (toEnum l) >> putByteString body)

class IsRequest a where
  getApiKey :: a -> ApiKey
  putRequest :: a -> Put
  getApiVersion :: a -> Int16

instance IsRequest RawRequest where
  getApiKey = apiKey
  getApiVersion = apiVersion
  putRequest = putRawRequest
