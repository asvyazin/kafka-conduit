module Kafka.Messages.MessageSet where

import Kafka.Messages.Utils

import Control.Applicative
import Data.ByteString
import qualified Data.ByteString as B (length)
import Data.Int
import Data.Serialize.Get hiding (getBytes)
import Data.Serialize.Put

type MessageSet = [MessageSetItem]

putMessageSet :: MessageSet -> Put
putMessageSet = putBytes . runPut . mapM_ putMessageSetItem

getMessageSet :: Get MessageSet
getMessageSet = do
  messageSetSize <- fromEnum <$> getInt32be
  getMessageSetWithSize messageSetSize

getMessageSetWithSize :: Int -> Get MessageSet
getMessageSetWithSize size | size < 0 = error "Invalid messageSet size"
                           | otherwise = do
                             item <- getMessageSetItem
                             items <- getMessageSetWithSize (size - sizeofMessageSetItem item)
                             return (item : items)

data MessageSetItem = MessageSetItem { messageOffset :: Int64
                                     , message :: Message } deriving (Eq, Show)

putMessageSetItem :: MessageSetItem -> Put
putMessageSetItem m = let messageBytes = runPut $ putMessage $ message m in
  putInt64be (messageOffset m) >> putBytes messageBytes

getMessageSetItem :: Get MessageSetItem
getMessageSetItem = MessageSetItem <$> getInt64be <*> getMessage

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

getMessage :: Get Message
getMessage = Message <$> getInt32be <*> getInt8 <*> getInt8 <*> getBytes <*> getBytes

sizeofMessageSetItem :: MessageSetItem -> Int
sizeofMessageSetItem m = sizeofInt64 + sizeofMessage (message m)

sizeofMessage :: Message -> Int
sizeofMessage m = 3 * sizeofInt32 + 2 * sizeofInt8 + B.length (messageKey m) + B.length (messageValue m)

sizeofInt64 :: Int
sizeofInt64 = 8

sizeofInt32 :: Int
sizeofInt32 = 4

sizeofInt8 :: Int
sizeofInt8 = 1
