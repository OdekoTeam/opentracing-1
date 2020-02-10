{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module OpenTracing.DataDog where

import Control.Concurrent
import Control.Lens ((^.))
import Control.Monad
import Control.Monad.IO.Class
import Data.Aeson
import qualified Data.ByteString.Base64.Lazy as B64
import Data.HashMap.Strict (HashMap)
import qualified Data.HashTable.IO as HT
import Data.Proxy
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.Encoding as LT
import Data.Time
import Data.Time.Clock.POSIX
import Data.Word
import GHC.Generics
import Network.HTTP.Client (Manager)
import OpenTracing
import Servant.API
import Servant.Client
import System.Random

data DataDog m = DataDog
  { sendTraces :: [DataDogSpan] -> m ()
  }

type DataDogAPI = "v0.4"
  :> "traces"
  :> ReqBody '[JSON] [[DataDogSpan]] :> Put '[JSON] Value

dataDogAPI :: Proxy DataDogAPI
dataDogAPI = Proxy

putTraces
  :: ClientEnv
  -> [[DataDogSpan]]
  -> IO (Either ServantError Value)
putTraces = flip $ runClientM . client dataDogAPI

defaultDataDogClientEnv
  :: Manager
  -> ClientEnv
defaultDataDogClientEnv mgr = mkClientEnv mgr BaseUrl
  { baseUrlScheme = Http
  , baseUrlHost = "localhost"
  , baseUrlPort = 8126
  , baseUrlPath = ""
  }

defaultHTTPDataDog :: MonadIO m => Manager -> m (DataDog m)
defaultHTTPDataDog = traceCollecting . httpDataDog . defaultDataDogClientEnv

traceCollecting :: MonadIO m => DataDog m -> m (DataDog m)
traceCollecting dd = do
  table :: HT.BasicHashTable Word64 [DataDogSpan] <- liftIO HT.new
  return $ DataDog
    { sendTraces = \spans -> do
        forM_ spans $ \s -> do
          mGroup <- liftIO $ HT.lookup table (traceId s)
          case mGroup of
            Nothing -> liftIO $ HT.insert table (traceId s) [s]
            Just group -> liftIO $ HT.insert table (traceId s) (s : group)
        forM_ spans $ \s -> do
          when (parentId s == Nothing) $ do
            mGroup <- liftIO $ HT.lookup table (traceId s)
            case mGroup of
              Nothing -> return ()
                         -- ^ this shouldn't happen since we
                         -- just inserted
              Just group -> sendTraces dd group
            liftIO $ HT.delete table (traceId s)
    }

httpDataDog :: MonadIO m => ClientEnv -> DataDog m
httpDataDog env = DataDog
  { sendTraces = \spans -> void . liftIO . forkIO . void $
      putTraces env [spans]
  }

data DataDogTraceEnv = DataDogTraceEnv
  { datadogService :: Text
  , datadogResource :: Text
  }

data DataDogSpan = DataDogSpan
  { traceId :: Word64
  , spanId :: Word64
  , name :: Text
  , resource :: Text
  , service :: Text
  , start :: NominalDiffTime
  , duration :: NominalDiffTime
  , parentId :: Maybe Word64
  , meta :: HashMap Text Text
  } deriving (Generic, Show)

instance ToJSON DataDogSpan where
  toJSON ddSpan = object
    [ "trace_id" .= traceId ddSpan
    , "span_id" .= spanId ddSpan
    , "name" .= name ddSpan
--    , "resource" .= resource ddSpan
    , "service" .= service ddSpan
    , "start" .= (floor @_ @Integer $ 10^9 * start ddSpan)
    , "duration" .= (floor @_ @Integer $ 10^9 * duration ddSpan)
    , "parent_id" .= parentId ddSpan
    , "meta" .= meta ddSpan
    ]

datadogReporter
  :: forall m
   . DataDog m
  -> DataDogTraceEnv
  -> FinishedSpan
  -> m ()
datadogReporter DataDog{sendTraces} env otSpan = sendTraces $
  [ DataDogSpan
    { traceId = traceIdLo . ctxTraceID $ otSpan ^. spanContext
    , spanId = ctxSpanID $ otSpan ^. spanContext
    , name = otSpan ^. spanOperation
    , resource = datadogResource env
    , service = datadogService env
    , start = utcTimeToPOSIXSeconds $ otSpan ^. spanStart
    , duration = otSpan ^. spanDuration
    , parentId = ctxParentSpanID $ otSpan ^. spanContext
    , meta = fmap tagValToText . fromTags $ otSpan ^. spanTags
    }
  ]

tshow :: Show a => a -> Text
tshow = T.pack . show

tagValToText :: TagVal -> Text
tagValToText (BoolT b) = tshow b
tagValToText (StringT s) = s
tagValToText (IntT i) = tshow i
tagValToText (DoubleT d) = tshow d
tagValToText (BinaryT b) = LT.toStrict . LT.decodeUtf8 $ B64.encode b
