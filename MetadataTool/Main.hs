{-# LANGUAGE OverloadedStrings #-}

module Main where

import Kafka.BrokerConnection
import Kafka.Messages.MetadataRequest
import Kafka.Messages.Request
import Kafka.Messages.Response

import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.State
import Data.ByteString (ByteString)
import Data.Conduit
import Data.Conduit.Network

testClientId :: ByteString
testClientId = "testClient"

logC :: (MonadIO m, Show a) => Conduit a m a
logC = awaitForever $ \x -> do
  liftIO $ print x
  yield x

main :: IO ()
main = runTCPClient (clientSettings 9092 "localhost") $ \appData -> do
  let rawRequests = transPipe lift $ sendRawRequests =$= appSink appData
      rawResponses = transPipe lift $ appSource appData =$= receiveRawResponses
  flip evalStateT emptyWaitingRequests $ do
    yield (MetadataRequestMessage $ MetadataRequest ["test"]) $$ sendRequestMessage =$= rawRequests
    rawResponses =$= receiveResponseMessage $$ (await >>= liftIO . print)
