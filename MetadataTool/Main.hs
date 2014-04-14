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
  conn <- brokerConnection "testClient" appData
  yield (MetadataRequestMessage $ MetadataRequest ["test"]) $$ requestMessageSink conn
  responseMessageSource conn $$ (await >>= liftIO . print)
