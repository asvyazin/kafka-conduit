module Kafka.Messages.MessageSet where

import Kafka.Messages.Request

import Data.ByteString
import Data.Int
import Data.Serialize.Put

type MessageSet = [MessageSetItem]

putMessageSet :: MessageSet -> Put
putMessageSet = putArray putMessageSetItem

data MessageSetItem = MessageSetItem { messageOffset :: Int64
                                     , message :: Message } deriving (Eq, Show)

putMessageSetItem :: MessageSetItem -> Put
putMessageSetItem m = putInt64be (messageOffset m) >> putMessage (message m)

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
