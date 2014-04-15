{-# LANGUAGE OverloadedStrings #-}

module Main where

import Kafka.BrokerConnection
import Kafka.Messages.MetadataRequest

import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import Data.Conduit
import Data.Conduit.Network
import Network.Socket

testClientId :: ByteString
testClientId = "testClient"

testTopicName :: ByteString
testTopicName = "test"

logC :: (MonadIO m, Show a) => Conduit a m a
logC = awaitForever $ \x -> do
  liftIO $ print x
  yield x

main :: IO ()
main = withSocketsDo $ runTCPClient (clientSettings 9092 "localhost") $ \appData -> do
  conn <- brokerConnection testClientId appData
  yield (MetadataRequestMessage $ MetadataRequest [testTopicName]) $$ requestMessageSink conn
  responseMessageSource conn $$ (await >>= liftIO . print)
