{-# LANGUAGE OverloadedStrings, RankNTypes #-}

module Kafka.BrokerConnection (BrokerConnection
                              , withBrokerConnection
                              , requestAsync
                              , RequestMessage(..)
                              , ResponseMessage(..)) where

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
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.TMVar
import Control.Concurrent.STM.TVar
import Control.Monad.Trans.Class
import Control.Monad.Trans.State
import Data.ByteString
import Data.Conduit
import qualified Data.Conduit as C (yield)
import Data.Conduit.Network
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

data WaitingRequest = WaitingRequest { requestApiKey :: ApiKey
                                     , responsePromise :: TMVar ResponseMessage }
                      
data WaitingRequestsStore = WaitingRequestsStore { currentCorrelationId :: Int32
                                                 , requests :: M.Map Int32 WaitingRequest }

emptyWaitingRequests :: WaitingRequestsStore
emptyWaitingRequests = WaitingRequestsStore 0 M.empty

putWaitingRequest :: WaitingRequestsStore -> WaitingRequest -> (Int32, WaitingRequestsStore)
putWaitingRequest (WaitingRequestsStore c r) k = (c, WaitingRequestsStore (c + 1) (M.insert c k r))

getWaitingRequest :: WaitingRequestsStore -> Int32 -> (WaitingRequest, WaitingRequestsStore)
getWaitingRequest s@(WaitingRequestsStore {requests = r}) c = (fromJust $ M.lookup c r, s {requests = M.delete c r})

data BrokerConnection = BrokerConnection { waitingRequests :: TVar WaitingRequestsStore
                                         , appData :: AppData
                                         , connectionClientId :: ByteString
                                         , responsesThread :: ThreadId }

brokerConnection :: ByteString -> AppData -> IO BrokerConnection
brokerConnection cid ad = do
  w <- atomically $ newTVar emptyWaitingRequests
  thread <- forkIO $ receiveResponsesLoop w ad
  return $ BrokerConnection w ad cid thread

withBrokerConnection :: ByteString -> AppData -> (BrokerConnection -> IO a) -> IO a
withBrokerConnection cid ad callback = do
  w <- atomically $ newTVar emptyWaitingRequests
  thread <- forkIO $ receiveResponsesLoop w ad
  result <- callback $ BrokerConnection w ad cid thread
  killThread thread
  return result

receiveResponsesLoop :: TVar WaitingRequestsStore -> AppData -> IO ()
receiveResponsesLoop requests app = doReceiveResponse requests app >> receiveResponsesLoop requests app

getWaitingRequestSTM :: TVar WaitingRequestsStore -> Int32 -> STM WaitingRequest
getWaitingRequestSTM store corrId = do
  w <- readTVar store
  let (key, newW) = getWaitingRequest w corrId
  writeTVar store newW
  return key

putWaitingRequestSTM :: TVar WaitingRequestsStore -> WaitingRequest -> STM Int32
putWaitingRequestSTM store key = do
  w <- readTVar store
  let (corrId, newW) = putWaitingRequest w key
  writeTVar store newW
  return corrId

doReceiveResponse :: TVar WaitingRequestsStore -> AppData -> IO ()
doReceiveResponse requests app = appSource app =$= receiveRawResponses $$ (await >>= (lift . atomically . doReceiveResponseSTM requests . fromJust))

doReceiveResponseSTM :: TVar WaitingRequestsStore -> RawResponse -> STM ()
doReceiveResponseSTM requests raw = do
  reqData <- getWaitingRequestSTM requests $ Resp.correlationId raw
  case deserializeResponseMessage (responseMessageBytes raw) (requestApiKey reqData) of
    Left _ -> return ()
    Right message -> do
      putTMVar (responsePromise reqData) message

requestAsync :: BrokerConnection -> RequestMessage -> IO (TMVar ResponseMessage)
requestAsync conn req = let sink = sendRawRequests =$= appSink (appData conn) in do
  let key = getApiKey req
  let version = getApiVersion req
  let bytes = runPut $ putRequest req
  promise <- newEmptyTMVarIO
  corrId <- atomically $ putWaitingRequestSTM (waitingRequests conn) (WaitingRequest key promise)
  let rawRequest = RawRequest key version corrId (connectionClientId conn) bytes
  C.yield rawRequest $$ sink
  return promise
