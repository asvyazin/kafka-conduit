module Kafka.BrokerConnection where

import Kafka.Messages.ApiKey
import Kafka.Messages.MetadataResponse
import Kafka.Messages.ProduceResponse
import Kafka.Messages.Response

import Control.Applicative
import Control.Monad.State
import Data.ByteString
import Data.Conduit
import Data.Int
import qualified Data.Map as M
import Data.Serialize.Get

data WaitingRequestsStore = WaitingRequestsStore { currentCorrelationId :: Int32
                                                 , requests :: M.Map Int32 ApiKey } deriving (Eq, Show)

putWaitingRequest :: WaitingRequestsStore -> ApiKey -> (Int32, WaitingRequestsStore)
putWaitingRequest = undefined

getWaitingRequest :: WaitingRequestsStore -> Int32 -> (ApiKey, WaitingRequestsStore)
getWaitingRequest = undefined

data ResponseMessage = MetadataResponseMessage MetadataResponse
                     | ProduceResponseMessage ProduceResponse

deserializeResponseMessage :: ByteString -> ApiKey -> Either String ResponseMessage
deserializeResponseMessage bytes MetadataRequestApiKey = MetadataResponseMessage <$> runGet getMetadataResponse bytes
deserializeResponseMessage bytes ProduceRequestApiKey = ProduceResponseMessage <$> runGet getProduceResponse bytes

receiveResponseMessage :: Monad m => Conduit RawResponse (StateT WaitingRequestsStore m) ResponseMessage
receiveResponseMessage = awaitForever $ \raw -> do
  w <- get
  let (apiKey, newW) = getWaitingRequest w $ correlationId raw
  put newW
  case deserializeResponseMessage (responseMessageBytes raw) apiKey of
    Left _ -> return ()
    Right message -> yield message
