module Kafka.Messages.MessageSet where

import Kafka.Messages.Utils

import Data.ByteString
import Data.Int
import Data.Serialize.Put

type MessageSet = [MessageSetItem]

putMessageSet :: MessageSet -> Put
putMessageSet = putArray putMessageSetItem

data MessageSetItem = MessageSetItem { messageOffset :: Int64
                                     , message :: Message } deriving (Eq, Show)

putMessageSetItem :: MessageSetItem -> Put
putMessageSetItem m = let messageBytes = runPut $ putMessage $ message m in
  putInt64be (messageOffset m) >> putBytes messageBytes

data Message = Message { messageCrc :: Int32
                       , messageMagicByte :: Int8
                       , messageAttributes :: Int8
                       , messageKey :: ByteString
                       , messageValue :: ByteString } deriving (Eq, Show)

putMessage :: Message -> Put
putMessage m = putInt32be (messageCrc m) >>
               putInt8 (messageMagicByte m) >>
               putInt8 (messageAttributes m) >>
               putBytes (messageKey m) >>
               putBytes (messageValue m)
