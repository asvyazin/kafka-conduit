module Kafka.Messages.Response where

import Kafka.Messages.Utils

import Control.Applicative
import qualified Data.Attoparsec as P
import Data.Attoparsec.Binary
import Data.ByteString
import Data.Conduit
import Data.Conduit.Attoparsec
import Data.Int

anyInt32be :: P.Parser Int32
anyInt32be = enum <$> anyWord32be

data RawResponse = RawResponse { correlationId :: Int32, responseMessageBytes :: ByteString } deriving (Eq, Show)

rawResponse :: P.Parser RawResponse
rawResponse = do
  len <- fromEnum <$> anyWord32be
  RawResponse <$> anyInt32be <*> P.take (len - 4)

receiveRawResponses :: MonadThrow m => Conduit ByteString m RawResponse
receiveRawResponses = mapOutput snd $ conduitParser rawResponse
