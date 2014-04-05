{-# LANGUAGE OverloadedStrings #-}

module Main where

import Kafka.Messages.MetadataRequest
import Kafka.Messages.MetadataResponse
import Kafka.Messages.Request
import Kafka.Messages.Response

import Control.Monad.IO.Class
import Data.Serialize.Get
import Data.Serialize.Put
import Data.ByteString (ByteString)
import Data.Conduit
import Data.Conduit.Network
import Data.Maybe

testClientId :: ByteString
testClientId = "testClient"

logC :: (MonadIO m, Show a) => Conduit a m a
logC = awaitForever $ \x -> do
  liftIO $ print x
  yield x

main :: IO ()
main = runTCPClient (clientSettings 9092 "localhost") $ \appData -> do
  let rawRequests = sendRawRequests =$= (appSink appData) :: Consumer RawRequest IO ()
      rawResponses = (appSource appData) =$= receiveRawResponses :: Producer IO RawResponse
  (yield $ RawRequest MetadataRequestApiKey 0 0 testClientId $ runPut $ putMetadataRequest $ MetadataRequest ["test"])
    $$ rawRequests
  rawResponses $$ do
    maybeRawResp <- await
    liftIO $ print $ runGet getMetadataResponse $ responseMessageBytes $ fromJust maybeRawResp
