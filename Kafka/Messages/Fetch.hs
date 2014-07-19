module Kafka.Messages.Fetch (
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
import Kafka.Messages.MessageSet
import Kafka.Messages.Utils

import Control.Applicative
import Data.ByteString
import Data.Int
import Data.Serialize.Put
import Data.Serialize.Get

apiKey :: ApiKey.ApiKey
apiKey = ApiKey.Fetch

apiVersion :: Int16
apiVersion = 0

data Request = Request { requestReplicaId :: Int32
                       , requestMaxWaitTime :: Int32
                       , requestMinBytes :: Int32
                       , requestTopics :: [RequestTopicItem] }
             deriving (Eq, Show)

putRequest :: Request -> Put
putRequest r = putInt32be (requestReplicaId r) >>
               putInt32be (requestMaxWaitTime r) >>
               putInt32be (requestMinBytes r) >>
               putArray putRequestTopicItem (requestTopics r)

data RequestTopicItem = RequestTopicItem { requestTopicName :: ByteString
                                         , requestPartitions :: [RequestPartitionItem] }
                      deriving (Eq, Show)

putRequestTopicItem :: RequestTopicItem -> Put
putRequestTopicItem r = putString (requestTopicName r) >>
                        putArray putRequestPartitionItem (requestPartitions r)

data RequestPartitionItem = RequestPartitionItem { requestPartitionId :: Int32
                                                 , requestFetchOffset :: Int64
                                                 , requestMaxBytes :: Int32 }
                          deriving (Eq, Show)

putRequestPartitionItem :: RequestPartitionItem -> Put
putRequestPartitionItem r = putInt32be (requestPartitionId r) >>
                            putInt64be (requestFetchOffset r) >>
                            putInt32be (requestMaxBytes r)

data Response = Response { fetchResponseTopics :: [ResponseTopicItem] } deriving (Eq, Show)

getResponse :: Get Response
getResponse = Response <$> getArray getResponseTopicItem

data ResponseTopicItem = ResponseTopicItem { responseTopicName :: ByteString
                                           , responsePartitions :: [ResponsePartitionItem] }
                       deriving (Eq, Show)

getResponseTopicItem :: Get ResponseTopicItem
getResponseTopicItem = ResponseTopicItem <$> getString <*> getArray getResponsePartitionItem

data ResponsePartitionItem = ResponsePartitionItem { responsePartitionId :: Int32
                                                   , responseErrorCode :: ErrorCode
                                                   , responseHighwaterMarkOffset :: Int64
                                                   , responseMessages :: MessageSet }
                           deriving (Eq, Show)

getResponsePartitionItem :: Get ResponsePartitionItem
getResponsePartitionItem = ResponsePartitionItem <$> getInt32be <*> getErrorCode <*> getInt64be <*> getMessageSet
