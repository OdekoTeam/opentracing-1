{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE PolyKinds #-}

module OpenTracing.Servant where

import Control.Applicative
import Servant (FromHttpApiData(..), Capture, (:>), Verb, Proxy(..), (:<|>), ReqBody', Description, QueryParam', QueryParams)
import Data.Text (Text)
import qualified Data.Text as T
import GHC.TypeLits
import Data.Function
import           Control.Lens            (over, set, view)
import           Data.Maybe
import           Data.Semigroup
import qualified Data.Text               as Text
import           Data.Text.Encoding      (decodeUtf8)
import Data.Vault.Lazy (Vault, Key)
import qualified Data.Vault.Lazy as V
import           Network.Wai
import           OpenTracing
import qualified OpenTracing.Propagation as Propagation
import qualified OpenTracing.Tracer      as Tracer
import           Prelude                 hiding (span)


class ParsePath api where
  parsePathDescription :: Proxy api -> [Text] -> Maybe Text

instance ParsePath (Verb method status ctypes a) where
  parsePathDescription _ [] = Just "/"
  parsePathDescription _ _ = Nothing -- don't accept the path if pieces are left over

instance (KnownSymbol path, ParsePath api) => ParsePath (path :> api)  where
  parsePathDescription _ (x:xs)
    | x == T.pack (symbolVal $ Proxy @path) = parsePathDescription (Proxy @api) xs
        & fmap (\rest -> "/" <> T.pack (symbolVal $ Proxy @path) <> rest)
  parsePathDescription _ _ = Nothing

instance (ParsePath api, KnownSymbol capture, FromHttpApiData a) => ParsePath (Capture capture a :> api) where
  parsePathDescription _ (x:xs)
    | Right _ <- parseUrlPiece @a x = parsePathDescription (Proxy @api) xs
        & fmap (\rest -> "/:" <> T.pack (symbolVal $ Proxy @capture) <> rest)
  parsePathDescription _ _ = Nothing

instance (ParsePath api) => ParsePath (Vault :> api) where
  parsePathDescription _ xs = parsePathDescription (Proxy @api) xs

instance (ParsePath l, ParsePath r) => ParsePath (l :<|> r) where
  parsePathDescription _ xs = parsePathDescription (Proxy @l) xs <|> parsePathDescription (Proxy @r) xs

instance (ParsePath api) => ParsePath (ReqBody' x y z :> api) where
  parsePathDescription _ xs = parsePathDescription (Proxy @api) xs

instance (ParsePath api) => ParsePath (Description t :> api) where
  parsePathDescription _ xs = parsePathDescription (Proxy @api) xs

instance (ParsePath api) => ParsePath (QueryParam' x y z :> api) where
  parsePathDescription _ xs = parsePathDescription (Proxy @api) xs

instance (ParsePath api) => ParsePath (QueryParams y z :> api) where
  parsePathDescription _ xs = parsePathDescription (Proxy @api) xs


type TracedApplication = ActiveSpan -> Application

parentSpanKey :: IO (Key ActiveSpan)
parentSpanKey = V.newKey

data OpenTracingEnv api p = OpenTracingEnv
  { opentracingAPIProxy :: Proxy api
  , opentracingActiveSpanKey :: Key ActiveSpan
  , opentracingTracer :: Tracer
  , opentracingPropagation :: Propagation p
  , opentracingResourceTag :: Maybe (Text -> Tag)
  }

opentracing
    :: (HasCarrier Headers p, ParsePath api)
    => OpenTracingEnv api p
    -> TracedApplication
    -> Application
opentracing env app req respond = do
    let propagation = opentracingPropagation env
        api = opentracingAPIProxy env
        tracer = opentracingTracer env
        vaultKey = opentracingActiveSpanKey env
        ctx = Propagation.extract propagation (requestHeaders req)
        opt = let name = "servant.request"
                  resource = parsePathDescription api $ pathInfo req
                  refs = (\x -> set refPropagated x mempty)
                       . maybeToList . fmap ChildOf $ ctx
               in set spanOptSampled (view ctxSampled <$> ctx)
                . set spanOptTags
                      ([ HttpMethod  (requestMethod req)
                       , HttpUrl     (decodeUtf8 url)
                       , PeerAddress (Text.pack (show (remoteHost req))) -- not so great
                       , SpanKind    RPCServer
                       ] ++ catMaybes
                       [ opentracingResourceTag env <*> resource
                       ])
                $ spanOpts name refs

    Tracer.traced_ tracer opt $ \span -> do
      let oldVault = vault req
          newVault = V.insert vaultKey span oldVault
          newReq = req { vault = newVault }
      app span newReq $ \res -> do
        modifyActiveSpan span $
            over spanTags (setTag (HttpStatusCode (responseStatus res)))
        respond res
  where
    url = "http" <> if isSecure req then "s" else mempty <> "://"
       <> fromMaybe "localhost" (requestHeaderHost req)
       <> rawPathInfo req <> rawQueryString req
