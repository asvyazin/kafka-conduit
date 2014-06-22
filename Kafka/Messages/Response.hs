module Kafka.Messages.Response where

import Kafka.Messages.Utils

import Control.Applicative
import Control.Monad.Catch
import Data.ByteString
import Data.Conduit
import Data.Conduit.Cereal
import Data.Int
import Data.Serialize
import Data.Serialize.Get

data RawResponse = RawResponse { correlationId :: Int32, responseMessageBytes :: ByteString } deriving (Eq, Show)

rawResponse :: Get RawResponse
rawResponse = do
  len <- fromEnum <$> getWord32be
  RawResponse <$> getInt32be <*> getByteString (len - 4)

receiveRawResponses :: MonadThrow m => Conduit ByteString m RawResponse
receiveRawResponses = conduitGet rawResponse
