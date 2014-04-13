{-# LANGUAGE OverloadedStrings #-}

module Kafka.BrokerConnection where

import Kafka.Messages.ApiKey
import Kafka.Messages.MetadataRequest
import Kafka.Messages.MetadataResponse
import Kafka.Messages.ProduceRequest
import Kafka.Messages.ProduceResponse
import Kafka.Messages.FetchRequest
import Kafka.Messages.FetchResponse
import Kafka.Messages.OffsetRequest
import Kafka.Messages.OffsetResponse
import Kafka.Messages.Request
import qualified Kafka.Messages.Response as Resp (correlationId)
import Kafka.Messages.Response

import Control.Applicative
import Control.Monad.Trans.Class
import Control.Monad.Trans.State
import Data.ByteString
import Data.Conduit
import Data.Int
import qualified Data.Map as M
import Data.Maybe
import Data.Serialize.Get
import Data.Serialize.Put

data RequestMessage = MetadataRequestMessage MetadataRequest
                    | ProduceRequestMessage ProduceRequest
                    | FetchRequestMessage FetchRequest
                    | OffsetRequestMessage OffsetRequest
                    deriving (Eq, Show)
                    
instance IsRequest RequestMessage where
  getApiKey (MetadataRequestMessage m) = getApiKey m
  getApiKey (ProduceRequestMessage m) = getApiKey m
  getApiKey (FetchRequestMessage m) = getApiKey m
  getApiKey (OffsetRequestMessage m) = getApiKey m
  putRequest (MetadataRequestMessage m) = putRequest m
  putRequest (ProduceRequestMessage m) = putRequest m
  putRequest (FetchRequestMessage m) = putRequest m
  putRequest (OffsetRequestMessage m) = putRequest m
  getApiVersion (MetadataRequestMessage m) = getApiVersion m
  getApiVersion (ProduceRequestMessage m) = getApiVersion m
  getApiVersion (FetchRequestMessage m) = getApiVersion m
  getApiVersion (OffsetRequestMessage m) = getApiVersion m

data ResponseMessage = MetadataResponseMessage MetadataResponse
                     | ProduceResponseMessage ProduceResponse
                     | FetchResponseMessage FetchResponse
                     | OffsetResponseMessage OffsetResponse
                     deriving (Eq, Show)

deserializeResponseMessage :: ByteString -> ApiKey -> Either String ResponseMessage
deserializeResponseMessage bytes MetadataRequestApiKey = MetadataResponseMessage <$> runGet getMetadataResponse bytes
deserializeResponseMessage bytes ProduceRequestApiKey = ProduceResponseMessage <$> runGet getProduceResponse bytes
deserializeResponseMessage bytes FetchRequestApiKey = FetchResponseMessage <$> runGet getFetchResponse bytes
deserializeResponseMessage bytes OffsetRequestApiKey = OffsetResponseMessage <$> runGet getOffsetResponse bytes
                       
data WaitingRequestsStore = WaitingRequestsStore { currentCorrelationId :: Int32
                                                 , requests :: M.Map Int32 ApiKey } deriving (Eq, Show)

emptyWaitingRequests :: WaitingRequestsStore
emptyWaitingRequests = WaitingRequestsStore 0 M.empty

putWaitingRequest :: WaitingRequestsStore -> ApiKey -> (Int32, WaitingRequestsStore)
putWaitingRequest (WaitingRequestsStore c r) k = (c, WaitingRequestsStore (c + 1) (M.insert c k r))

getWaitingRequest :: WaitingRequestsStore -> Int32 -> (ApiKey, WaitingRequestsStore)
getWaitingRequest s@(WaitingRequestsStore {requests = r}) c = (fromJust $ M.lookup c r, s {requests = M.delete c r})

receiveResponseMessage :: Monad m => Conduit RawResponse (StateT WaitingRequestsStore m) ResponseMessage
receiveResponseMessage = awaitForever $ \raw -> do
  w <- lift get
  let (key, newW) = getWaitingRequest w $ Resp.correlationId raw
  lift $ put newW
  case deserializeResponseMessage (responseMessageBytes raw) key of
    Left _ -> return ()
    Right message -> yield message

sendRequestMessage :: Monad m => Conduit RequestMessage (StateT WaitingRequestsStore m) RawRequest
sendRequestMessage = awaitForever $ \req -> do
  w <- lift get
  let key = getApiKey req
  let version = getApiVersion req
  let (corrId, newW) = putWaitingRequest w key
  lift $ put newW
  yield $ RawRequest key version corrId "testClient" $ runPut $ putRequest req
