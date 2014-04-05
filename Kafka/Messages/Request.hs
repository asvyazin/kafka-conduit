module Kafka.Messages.Request where

import Kafka.Messages.ApiKey
import Kafka.Messages.Utils

import Data.Serialize.Put
import qualified Data.ByteString as B
import Data.Conduit
import qualified Data.Conduit.Combinators as C
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

putRawRequestWithPrefix :: RawRequest -> Put
putRawRequestWithPrefix r = let body = runPut $ putRawRequest r
                                l = B.length body
                            in putWord32be (toEnum l) >> putByteString body

sendRawRequests :: Monad m => Conduit RawRequest m B.ByteString
sendRawRequests = C.map $ runPut . putRawRequestWithPrefix
