{-# LANGUAGE OverloadedStrings #-}

module Kafka.BrokerConnection where

import Kafka.Messages.ApiKey
import Kafka.Messages.MetadataRequest
import Kafka.Messages.MetadataResponse
import Kafka.Messages.ProduceRequest
import Kafka.Messages.ProduceResponse
import Kafka.Messages.Request
import qualified Kafka.Messages.Response as Resp (correlationId)
import Kafka.Messages.Response

import Control.Applicative
import Control.Monad.State
import Data.ByteString
import Data.Conduit
import Data.Int
import qualified Data.Map as M
import Data.Maybe
import Data.Serialize.Get
import Data.Serialize.Put

data RequestMessage = MetadataRequestMessage MetadataRequest
                    | ProduceRequestMessage ProduceRequest
                    deriving (Eq, Show)

getApiKey :: RequestMessage -> ApiKey
getApiKey (MetadataRequestMessage _) = MetadataRequestApiKey
getApiKey (ProduceRequestMessage _) = ProduceRequestApiKey
                      
data ResponseMessage = MetadataResponseMessage MetadataResponse
                     | ProduceResponseMessage ProduceResponse
                     deriving (Eq, Show)
                       
data WaitingRequestsStore = WaitingRequestsStore { currentCorrelationId :: Int32
                                                 , requests :: M.Map Int32 ApiKey } deriving (Eq, Show)

emptyWaitingRequests :: WaitingRequestsStore
emptyWaitingRequests = WaitingRequestsStore 0 M.empty

putWaitingRequest :: WaitingRequestsStore -> ApiKey -> (Int32, WaitingRequestsStore)
putWaitingRequest (WaitingRequestsStore c r) k = (c, WaitingRequestsStore (c + 1) (M.insert c k r))

getWaitingRequest :: WaitingRequestsStore -> Int32 -> (ApiKey, WaitingRequestsStore)
getWaitingRequest s@(WaitingRequestsStore {requests = r}) c = (fromJust $ M.lookup c r, s {requests = M.delete c r})

putRequestMessage :: RequestMessage -> Put
putRequestMessage (MetadataRequestMessage m) = putMetadataRequest m
putRequestMessage (ProduceRequestMessage m) = putProduceRequest m

deserializeResponseMessage :: ByteString -> ApiKey -> Either String ResponseMessage
deserializeResponseMessage bytes MetadataRequestApiKey = MetadataResponseMessage <$> runGet getMetadataResponse bytes
deserializeResponseMessage bytes ProduceRequestApiKey = ProduceResponseMessage <$> runGet getProduceResponse bytes

receiveResponseMessage :: Monad m => Conduit RawResponse (StateT WaitingRequestsStore m) ResponseMessage
receiveResponseMessage = awaitForever $ \raw -> do
  w <- get
  let (key, newW) = getWaitingRequest w $ Resp.correlationId raw
  put newW
  case deserializeResponseMessage (responseMessageBytes raw) key of
    Left _ -> return ()
    Right message -> yield message

sendRequestMessage :: Monad m => Conduit RequestMessage (StateT WaitingRequestsStore m) RawRequest
sendRequestMessage = awaitForever $ \req -> do
  w <- get
  let key = getApiKey req
  let (corrId, newW) = putWaitingRequest w key
  put newW
  yield $ RawRequest key 0 corrId "testClient" $ runPut $ putRequestMessage req