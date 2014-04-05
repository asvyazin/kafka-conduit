module Kafka.Messages.Request where

import Kafka.Messages.Utils

import Data.Serialize.Put
import qualified Data.ByteString as B
import Data.Conduit
import qualified Data.Conduit.Combinators as C
import Data.Int

putInt8 :: Int8 -> Put
putInt8 = putWord8 . enum

putInt16be :: Int16 -> Put
putInt16be = putWord16be . enum

putInt32be :: Int32 -> Put
putInt32be = putWord32be . enum

putInt64be :: Int64 -> Put
putInt64be = putWord64be . enum

putBytes :: B.ByteString -> Put
putBytes bytes = putWord32be (toEnum $ B.length bytes) >> putByteString bytes

putString :: B.ByteString -> Put
putString str = putWord16be (toEnum $ B.length str) >> putByteString str

putArray :: (a -> Put) -> [a] -> Put
putArray put arr = let l = length arr in
  putWord32be (toEnum l) >> mapM_ put arr

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
