module Kafka.Messages.Request where

import Kafka.Messages.Utils

import Data.Binary.Put
import qualified Data.ByteString as B
import Data.ByteString.Lazy (toStrict)
import Data.Conduit
import qualified Data.Conduit.Combinators as C
import Data.Int

putInt16be :: Int16 -> Put
putInt16be = putWord16be . enum

putInt32be :: Int32 -> Put
putInt32be = putWord32be . enum

putInt64be :: Int64 -> Put
putInt64be = putWord64be . enum

putBytes :: B.ByteString -> Put
putBytes bytes = putWord32be (toEnum $ B.length bytes) >> putByteString bytes

putString :: B.ByteString -> Put
putString = putBytes

data RawRequest = RawRequest { apiKey :: Int16
                             , apiVersion :: Int16
                             , correlationId :: Int32
                             , clientId :: B.ByteString
                             , requestMessageBytes :: B.ByteString } deriving (Eq, Show)

putRawRequest :: RawRequest -> Put
putRawRequest r = putInt16be (apiKey r)
                  >> putInt16be (apiVersion r)
                  >> putInt32be (correlationId r)
                  >> putString (clientId r)
                  >> putBytes (requestMessageBytes r)

sendRawRequests :: Monad m => Conduit RawRequest m B.ByteString
sendRawRequests = C.map $ toStrict . runPut . putRawRequest
