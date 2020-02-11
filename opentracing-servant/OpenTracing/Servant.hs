{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PatternSynonyms #-}

module OpenTracing.Servant where

import Servant (FromHttpApiData(..), Capture, (:>), Verb, Proxy(..))
import Data.Text (Text)
import qualified Data.Text as T
import GHC.TypeLits
import Data.Function
import           Control.Lens            (over, set, view)
import           Data.Maybe
import           Data.Semigroup
import qualified Data.Text               as Text
import           Data.Text.Encoding      (decodeUtf8)
import           Network.Wai
import           OpenTracing
import qualified OpenTracing.Propagation as Propagation
import qualified OpenTracing.Tracer      as Tracer
import OpenTracing.DataDog (pattern DataDogResource)
import           Prelude                 hiding (span)


class ParsePath api where
  parsePathDescription :: Proxy api -> [Text] -> Maybe Text

instance ParsePath (Verb method status ctypes a) where
  parsePathDescription _ [] = Just ""
  parsePathDescription _ _ = Nothing -- don't accept the path if pieces are left over

instance (KnownSymbol path, ParsePath api) => ParsePath (path :> api)  where
  parsePathDescription _ (x:xs)
    | x == T.pack (symbolVal $ Proxy @path) = parsePathDescription (Proxy @api) xs
        & fmap (\rest -> T.pack (symbolVal $ Proxy @path) <> "/" <> rest)
  parsePathDescription _ _ = Nothing

instance (ParsePath api, KnownSymbol capture, FromHttpApiData a) => ParsePath (Capture capture a :> api) where
  parsePathDescription _ (x:xs)
    | Right _ <- parseUrlPiece @a x = parsePathDescription (Proxy @api) xs
        & fmap (\rest -> ":" <> T.pack (symbolVal $ Proxy @capture) <> "/" <> rest)
  parsePathDescription _ _ = Nothing

type TracedApplication = ActiveSpan -> Application

opentracing
    :: (HasCarrier Headers p, ParsePath api)
    => Proxy api
    -> Tracer
    -> Propagation        p
    -> TracedApplication
    -> Application
opentracing api t p app req respond = do
    let ctx = Propagation.extract p (requestHeaders req)
    let opt = let name = "servant.request"
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
                       [ DataDogResource <$> resource
                       ])
                $ spanOpts name refs

    Tracer.traced_ t opt $ \span -> app span req $ \res -> do
        modifyActiveSpan span $
            over spanTags (setTag (HttpStatusCode (responseStatus res)))
        respond res
  where
    url = "http" <> if isSecure req then "s" else mempty <> "://"
       <> fromMaybe "localhost" (requestHeaderHost req)
       <> rawPathInfo req <> rawQueryString req
