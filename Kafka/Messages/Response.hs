module Kafka.Messages.Response where

import Kafka.Messages.Utils

import Control.Applicative
import Control.Monad
import Control.Monad.Catch
import qualified Data.Attoparsec as P
import Data.Attoparsec.Binary
import Data.Serialize.Get hiding (getBytes)
import Data.ByteString
import Data.Conduit
import Data.Conduit.Attoparsec
import Data.Int

type ErrorCode = Int16

anyInt32be :: P.Parser Int32
anyInt32be = enum <$> anyWord32be

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

getBytes :: Get ByteString
getBytes = (fromEnum <$> getWord32be) >>= getByteString

getString :: Get ByteString
getString = (fromEnum <$> getWord16be) >>= getByteString

data RawResponse = RawResponse { correlationId :: Int32, responseMessageBytes :: ByteString } deriving (Eq, Show)

rawResponse :: P.Parser RawResponse
rawResponse = do
  len <- fromEnum <$> anyWord32be
  RawResponse <$> anyInt32be <*> P.take (len - 4)

receiveRawResponses :: MonadThrow m => Conduit ByteString m RawResponse
receiveRawResponses = mapOutput snd $ conduitParser rawResponse
