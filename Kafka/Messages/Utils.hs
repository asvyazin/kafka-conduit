module Kafka.Messages.Utils where

import Control.Applicative
import Control.Monad
import qualified Data.ByteString as B
import Data.Int
import Data.Serialize.Get
import Data.Serialize.Put

enum :: (Enum a, Enum b) => a -> b
enum = toEnum . fromEnum

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

getInt64be :: Get Int64
getInt64be = enum <$> getWord64be

getInt32be :: Get Int32
getInt32be = enum <$> getWord32be

getInt16be :: Get Int16
getInt16be = enum <$> getWord16be

getArray :: Get a -> Get [a]
getArray get = do
  l <- fromEnum <$> getWord32be
  replicateM l get

getBytes :: Get B.ByteString
getBytes = (fromEnum <$> getWord32be) >>= getByteString

getString :: Get B.ByteString
getString = (fromEnum <$> getWord16be) >>= getByteString
