module Kafka.Messages.Produce (
  Request(..),
  RequestTopic(..),
  RequestPartition(..),
  Response(..),
  ResponseTopic(..),
  ResponsePartition(..),
  putRequest,
  getResponse,
  apiVersion,
  apiKey) where

import qualified Kafka.Messages.ApiKey as ApiKey
import Kafka.Messages.MessageSet
import Kafka.Messages.Utils

import Control.Applicative
import Data.ByteString
import Data.Int
import Data.Serialize.Put
import Data.Serialize.Get

apiVersion :: Int16
apiVersion = 0

apiKey :: ApiKey.ApiKey
apiKey = ApiKey.Produce

data Request = Request { requestRequiredAcks :: Int16
                       , requestTimeout :: Int32
                       , requestTopics :: [RequestTopic] } deriving (Eq, Show)

putRequest :: Request -> Put
putRequest r = putInt16be (requestRequiredAcks r) >>
               putInt32be (requestTimeout r) >>
               putArray putRequestTopic (requestTopics r)

data RequestTopic = RequestTopic { requestTopicName :: ByteString
                                 , requestPartitions :: [RequestPartition] } deriving (Eq, Show)

putRequestTopic :: RequestTopic -> Put
putRequestTopic r = putString (requestTopicName r) >>
                    putArray putRequestPartition (requestPartitions r)

data RequestPartition = RequestPartition { requestPartitionId :: Int32
                                         , requestMessages :: MessageSet } deriving (Eq, Show)

putRequestPartition :: RequestPartition -> Put
putRequestPartition r = putInt32be (requestPartitionId r) >>
                        putMessageSet (requestMessages r)

data Response = Response [ResponseTopic] deriving (Eq, Show)

getResponse :: Get Response
getResponse = Response <$> getArray getResponseTopic

data ResponseTopic = ResponseTopic { responseTopicName :: ByteString
                                   , responsePartitions :: [ResponsePartition] } deriving (Eq, Show)

getResponseTopic :: Get ResponseTopic
getResponseTopic = ResponseTopic <$> getString <*> getArray getResponsePartition

data ResponsePartition = ResponsePartition { responsePartitionId :: Int32
                                           , responsePartitionErrorCode :: ErrorCode
                                           , responsePartitionOffset :: Int64 } deriving (Eq, Show)

getResponsePartition :: Get ResponsePartition
getResponsePartition = ResponsePartition <$> getInt32be <*> getErrorCode <*> getInt64be
