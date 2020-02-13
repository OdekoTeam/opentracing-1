{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TupleSections #-}

module OpenTracing.DataDog
  ( datadogReporter
  , defaultHTTPDataDog
  , DataDogSpan(..)
  , DataDogTraceEnv(..)
  , DataDog(..)
  , traceCollecting
  , defaultDataDogClientEnv
  , httpDataDog
  , pattern DataDogResourceKey
  , pattern DataDogResource
  , _DataDogResource
  , datadogPropagation
  , mkClientEnv
  , BaseUrl(..)
  , Scheme(..)
  ) where

import Control.Concurrent
import Control.Lens ((^.), (&), Prism', prism', preview, review)
import Control.Monad
import Control.Monad.IO.Class
import Data.Aeson ((.=), ToJSON(..), Value, object)
import qualified Data.ByteString.Base64.Lazy as B64
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import qualified Data.HashTable.IO as HT
import Data.Maybe
import Data.Monoid
import qualified Data.List.NonEmpty as NE
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
import Text.Read

-- | An abstract interface to the DataDog agent
data DataDog m = DataDog
  { sendTraces :: [DataDogSpan] -> m ()
  }

-- | The HTTP API that the DataDog agent responds to
--
-- See https://docs.datadoghq.com/api/?lang=bash#tracing for more info
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

-- | A ClientEnv for the DataDog HTTP API with default values
-- assuming that the agent is running on the same host
-- (which it should be)
defaultDataDogClientEnv
  :: Manager
  -> ClientEnv
defaultDataDogClientEnv mgr = mkClientEnv mgr BaseUrl
  { baseUrlScheme = Http
  , baseUrlHost = "localhost"
  , baseUrlPort = 8126
  , baseUrlPath = ""
  }

-- | A good default implementation of the DataDog interface to use
-- when sending trace information to DataDog.
defaultHTTPDataDog :: MonadIO m => Manager -> m (DataDog m)
defaultHTTPDataDog = return . httpDataDog . defaultDataDogClientEnv

-- | The DataDog API accepts a a list of traces, where a trace
-- is itself a list of spans. Even though spans all have a unique
-- trace ID, datadog will display multiple traces if spans with
-- the same trace ID are submitted via multiple API calls, or
-- are otherwise not part of a single list of spans.
--
-- On the contrary to that, opentracing reports spans as they
-- finish, with no grouping of spans that share the same trace
-- ID.
--
-- In order to keep things neat and tidy in DataDog, each span
-- recorded will be stored in a hash map with the other spans
-- in the trace until the root span is finished, and then they
-- will all be sent to DataDog together.
traceCollecting :: MonadIO m => DataDog m -> m (DataDog m)
traceCollecting dd = do
  table :: HT.BasicHashTable Word64 [DataDogSpan] <- liftIO HT.new
  return $ DataDog
    { sendTraces = \spans -> do
        forM_ spans $ \s -> do
          mGroup <- liftIO $ HT.lookup table (traceId s)
          case mGroup of
            Nothing -> liftIO $ HT.insert table (traceId s) [s]
            Just spanGroup -> liftIO $ HT.insert table (traceId s) (s : spanGroup)
        forM_ spans $ \s -> do
          when (parentId s == Nothing) $ do
            mGroup <- liftIO $ HT.lookup table (traceId s)
            case mGroup of
              Nothing -> return ()
                         -- ^ this shouldn't happen since we
                         -- just inserted
              Just spanGroup -> sendTraces dd spanGroup
            liftIO $ HT.delete table (traceId s)
    }

-- | A DataDog implementation that will deliver spans to the DataDog
-- agent asynchronously via HTTP
httpDataDog :: MonadIO m => ClientEnv -> DataDog m
httpDataDog env = DataDog
  { sendTraces = \spans -> void . liftIO . forkIO . void $
      putTraces env [spans]
  }

-- | Additional info that DataDog desires for a span that is not part of the opentracing
-- standard.
data DataDogTraceEnv = DataDogTraceEnv
  { datadogService :: Text
  }

newtype Nanoseconds = Nanoseconds Integer
  deriving newtype (Eq, Show, ToJSON)

-- | Smart constructor for converting NominalDiffTime into nanoseconds.
toNanoseconds :: NominalDiffTime -> Nanoseconds
toNanoseconds diffTime = Nanoseconds . floor @_ @Integer $ 10^nano * diffTime
  where
    nano :: Int
    nano = 9

newtype ErrorPresent = ErrorPresent Bool
  deriving newtype (Eq, Show)

instance ToJSON ErrorPresent where
  toJSON (ErrorPresent True) = toJSON (1 :: Int)
  toJSON _ = toJSON $ Nothing @Int

-- | The representation DataDog expects for a single span
data DataDogSpan = DataDogSpan
  { traceId :: Word64
  , spanId :: Word64
  , name :: Text
  , resource :: Maybe Text
  , service :: Text
  , start :: Nanoseconds
  , duration :: Nanoseconds
  , parentId :: Maybe Word64
  , meta :: HashMap Text Text
  , errorPresent :: ErrorPresent
  } deriving (Generic, Show)

instance ToJSON DataDogSpan where
  toJSON ddSpan = object
    [ "trace_id" .= traceId ddSpan
    , "span_id" .= spanId ddSpan
    , "name" .= name ddSpan
    , "resource" .= resource ddSpan
    , "service" .= service ddSpan
    , "start" .= start ddSpan
    , "duration" .= duration ddSpan
    , "parent_id" .= parentId ddSpan
    , "meta" .= meta ddSpan
    , "error" .= errorPresent ddSpan
    ]

pattern DataDogResourceKey :: Text
pattern DataDogResourceKey = "resource.name"

pattern DataDogResource :: Text -> Tag
pattern DataDogResource v <- (preview _DataDogResource -> Just v) where
  DataDogResource v = review _DataDogResource v

_DataDogResource :: Prism' Tag Text
_DataDogResource = prism' ((DataDogResourceKey,) . StringT) $ \case
  (k, StringT v) | k == DataDogResourceKey -> Just v
  _ -> Nothing

-- | An opentracing reporter that hands spans off to a datadog agent
-- after massaging the data into the appropriate form.
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
    , resource = otSpan ^. spanTags
        & fromTags
        & HM.lookup DataDogResourceKey
        & fmap tagValToText
    , service = datadogService env
    , start = toNanoseconds . utcTimeToPOSIXSeconds $ otSpan ^. spanStart
    , duration = toNanoseconds $ otSpan ^. spanDuration
    , parentId = ctxParentSpanID $ otSpan ^. spanContext
    , meta = otSpan ^. spanTags
        & fromTags
        & fmap tagValToText
        & appendErrorLog
        & removeResourceName
    , errorPresent = otSpan ^. spanTags
        & getTagReify _Error ErrorKey
        & fromMaybe False
        & ErrorPresent
    }
  ]

  where
    removeResourceName = HM.delete DataDogResourceKey

    appendErrorLog hmap = hmap
      & HM.alter (\present -> getFirst $ First present <> firstErrorLog) "error.msg"

    firstErrorLog = otSpan ^. spanLogs
      & fmap (^. logFields)
      & fmap NE.toList
      & mconcat
      & (mapMaybe $ \case
          ErrObj e -> Just $ tshow e
          _ -> Nothing
        )
      & listToMaybe
      & First


datadogPropagation :: Propagation '[OpenTracing.Headers]
datadogPropagation = Carrier _DataDogHeaders :& RNil

_DataDogHeaders :: Prism' OpenTracing.Headers SpanContext
_DataDogHeaders = _HeadersTextMap . _DataDogTextMap

_DataDogTextMap :: Prism' TextMap SpanContext
_DataDogTextMap = prism' fromCtx toCtx
  where
    fromCtx :: SpanContext -> TextMap
    fromCtx ctx = HM.fromList
      [("x-datadog-trace-id", tshow . traceIdLo $ ctxTraceID ctx)
      ,("x-datadog-parent-id", tshow $ ctxSpanID ctx)
      ,("x-datadog-sampling-priority", fromSampled $ ctx ^. ctxSampled)
      ]

    fromSampled Sampled = "1"
    fromSampled _ = "0"

    toSampled "0" = Just NotSampled
    toSampled "1" = Just Sampled
    toSampled _ = Nothing


    toCtx :: TextMap -> Maybe SpanContext
    toCtx m = SpanContext
      <$> (TraceID Nothing
            <$> (HM.lookup "x-datadog-trace-id" m >>=
                 readMaybe . T.unpack
                )
          )
      <*> (HM.lookup "x-datadog-parent-id" m >>=
           readMaybe . T.unpack
          )
      <*> pure Nothing
      <*> (HM.lookup "x-datadog-sampling-priority"m >>=
           toSampled
          )
      <*> pure HM.empty

tshow :: Show a => a -> Text
tshow = T.pack . show

tagValToText :: TagVal -> Text
tagValToText (BoolT b) = tshow b
tagValToText (StringT s) = s
tagValToText (IntT i) = tshow i
tagValToText (DoubleT d) = tshow d
tagValToText (BinaryT b) = LT.toStrict . LT.decodeUtf8 $ B64.encode b
