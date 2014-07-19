{-# LANGUAGE OverloadedStrings, RankNTypes #-}

module Kafka.BrokerConnection (BrokerConnection
                              , withBrokerConnection
                              , requestAsync
                              , RequestMessage(..)
                              , ResponseMessage(..)) where

import Kafka.Messages.ApiKey
import qualified Kafka.Messages.Metadata as Metadata
import qualified Kafka.Messages.Produce as Produce
import qualified Kafka.Messages.Fetch as Fetch
import qualified Kafka.Messages.Offset as Offset
import qualified Kafka.Messages.Request as Request
import qualified Kafka.Messages.Response as Resp (correlationId)
import Kafka.Messages.Response

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad.Trans.Class
import Data.ByteString
import Data.Conduit
import qualified Data.Conduit as C (yield)
import qualified Data.Conduit.Combinators as CC (map)
import Data.Conduit.Network
import Data.Int
import qualified Data.Map as M
import Data.Maybe
import Data.Serialize.Get
import Data.Serialize.Put

data RequestMessage = MetadataRequestMessage Metadata.Request
                    | ProduceRequestMessage Produce.Request
                    | FetchRequestMessage Fetch.Request
                    | OffsetRequestMessage Offset.Request
                    deriving (Eq, Show)

apiKey :: RequestMessage -> ApiKey
apiKey (MetadataRequestMessage _) = Metadata.apiKey
apiKey (ProduceRequestMessage _) = Produce.apiKey
apiKey (FetchRequestMessage _) = Fetch.apiKey
apiKey (OffsetRequestMessage _) = Offset.apiKey

apiVersion :: RequestMessage -> Int16
apiVersion (MetadataRequestMessage _) = Metadata.apiVersion
apiVersion (ProduceRequestMessage _) = Produce.apiVersion
apiVersion (FetchRequestMessage _) = Fetch.apiVersion
apiVersion (OffsetRequestMessage _) = Offset.apiVersion

putRequest :: RequestMessage -> Put
putRequest (MetadataRequestMessage msg) = Metadata.putRequest msg
putRequest (ProduceRequestMessage msg) = Produce.putRequest msg
putRequest (FetchRequestMessage msg) = Fetch.putRequest msg
putRequest (OffsetRequestMessage msg) = Offset.putRequest msg

data ResponseMessage = MetadataResponseMessage Metadata.Response
                     | ProduceResponseMessage Produce.Response
                     | FetchResponseMessage Fetch.Response
                     | OffsetResponseMessage Offset.Response
                     deriving (Eq, Show)

deserializeResponseMessage :: ByteString -> ApiKey -> Either String ResponseMessage
deserializeResponseMessage bytes Metadata = MetadataResponseMessage <$> runGet Metadata.getResponse bytes
deserializeResponseMessage bytes Produce = ProduceResponseMessage <$> runGet Produce.getResponse bytes
deserializeResponseMessage bytes Fetch = FetchResponseMessage <$> runGet Fetch.getResponse bytes
deserializeResponseMessage bytes Offset = OffsetResponseMessage <$> runGet Offset.getResponse bytes

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
receiveResponsesLoop reqs app = doReceiveResponse reqs app >> receiveResponsesLoop reqs app

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
doReceiveResponse reqs app = appSource app =$= receiveRawResponses $$ (await >>= (lift . atomically . doReceiveResponseSTM reqs . fromJust))

doReceiveResponseSTM :: TVar WaitingRequestsStore -> RawResponse -> STM ()
doReceiveResponseSTM reqs raw = do
  reqData <- getWaitingRequestSTM reqs $ Resp.correlationId raw
  case deserializeResponseMessage (responseMessageBytes raw) (requestApiKey reqData) of
    Left _ -> return ()
    Right message -> putTMVar (responsePromise reqData) message

requestAsync :: BrokerConnection -> RequestMessage -> IO (TMVar ResponseMessage)
requestAsync conn req = let sink = CC.map Request.serializeRawRequest =$= appSink (appData conn) in do
  let key = apiKey req
  let version = apiVersion req
  let bytes = runPut $ putRequest req
  promise <- newEmptyTMVarIO
  corrId <- atomically $ putWaitingRequestSTM (waitingRequests conn) (WaitingRequest key promise)
  let rawRequest = Request.RawRequest key version corrId (connectionClientId conn) bytes
  C.yield rawRequest $$ sink
  return promise
