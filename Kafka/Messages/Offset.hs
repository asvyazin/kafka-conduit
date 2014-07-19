module Kafka.Messages.Offset (
  Request(..),
  RequestTopicItem(..),
  RequestPartitionItem(..),
  Response(..),
  ResponseTopicItem(..),
  ResponsePartitionItem(..),
  putRequest,
  getResponse,
  apiKey,
  apiVersion) where

import qualified Kafka.Messages.ApiKey as ApiKey
import Kafka.Messages.Utils

import Control.Applicative
import Data.ByteString
import Data.Int
import Data.Serialize.Put
import Data.Serialize.Get

apiKey :: ApiKey.ApiKey
apiKey = ApiKey.Offset

apiVersion :: Int16
apiVersion = 0

data Request = Request { requestReplicaId :: Int32
                       , requestTopics :: [RequestTopicItem] }
             deriving (Eq, Show)

putRequest :: Request -> Put
putRequest r = putInt32be (requestReplicaId r) >>
               putArray putRequestTopicItem (requestTopics r)

data RequestTopicItem = RequestTopicItem { requestTopicName :: ByteString
                                         , requestPartitions :: [RequestPartitionItem] }
                      deriving (Eq, Show)

putRequestTopicItem :: RequestTopicItem -> Put
putRequestTopicItem r = putString (requestTopicName r) >>
                        putArray putRequestPartitionItem (requestPartitions r)

data RequestPartitionItem = RequestPartitionItem { requestPartitionId :: Int32
                                                 , requestTime :: Int64
                                                 , requestMaxNumberOfOffsets :: Int32 }
                          deriving (Eq, Show)

putRequestPartitionItem :: RequestPartitionItem -> Put
putRequestPartitionItem r = putInt32be (requestPartitionId r) >>
                            putInt64be (requestTime r) >>
                            putInt32be (requestMaxNumberOfOffsets r)

data Response = Response { responseTopics :: [ResponseTopicItem] } deriving (Eq, Show)

getResponse :: Get Response
getResponse = Response <$> getArray getResponseTopicItem

data ResponseTopicItem = ResponseTopicItem { responseTopicName :: ByteString
                                           , responsePartitions :: [ResponsePartitionItem] }
                       deriving (Eq, Show)

getResponseTopicItem :: Get ResponseTopicItem
getResponseTopicItem = ResponseTopicItem <$> getString <*> getArray getResponsePartitionItem

data ResponsePartitionItem = ResponsePartitionItem { responsePartitionId :: Int32
                                                   , responseOffset :: Int64
                                                   , responseMetadata :: ByteString }
                           deriving (Eq, Show)

getResponsePartitionItem :: Get ResponsePartitionItem
getResponsePartitionItem = ResponsePartitionItem <$> getInt32be <*> getInt64be <*> getString
