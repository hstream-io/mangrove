{-# LANGUAGE DerivingStrategies    #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}

module Mangrove.Server
  ( processMsg
  ) where

import qualified Colog
import           Control.Monad.Reader  (liftIO)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.Text             as T
import           Data.Text.Encoding    (decodeUtf8, encodeUtf8)
import           Data.Word             (Word64)
import           Log.Store.Base        hiding (Env)
import qualified Network.HESP          as HESP
import qualified Network.Simple.TCP    as TCP
import           Network.Socket        (Socket)
import qualified Network.Socket        as NS

import qualified Mangrove.Store        as Store
import           Mangrove.Types        (App, RequestType (..), parseRequest)
import           Mangrove.Utils        ((.|.))

-------------------------------------------------------------------------------

-- | Parse one client request and then send one response to client.
processMsg :: Socket
           -> Context
           -> Either String HESP.Message
           -> App (Maybe ())
processMsg sock _ (Left errmsg) = do
  Colog.logError $ "Failed to parse message: " <> T.pack errmsg
  HESP.sendMsg sock $ HESP.mkSimpleError "ERR" $ (encodeUtf8 . T.pack) errmsg
  -- FIXME: should we close this connection?
  liftIO $ NS.gracefulClose sock (10 * 1000)  -- 10 seconds
  return Nothing
processMsg sock ctx (Right msg) =
  case parseRequest msg of
    Left e    -> do
      Colog.logWarning $ decodeUtf8 e
      HESP.sendMsg sock $ HESP.mkSimpleError "ERR" e
      return Nothing
    Right req -> processSPut sock ctx req .|. processSGet sock ctx req

-------------------------------------------------------------------------------
-- Process client requests

processSPut :: TCP.Socket -> Context -> RequestType -> App (Maybe ())
processSPut sock ctx (SPut topic payload) = do
  Colog.logInfo $ "Writing " <> decodeUtf8 topic <> " ..."
  r <- liftIO $ Store.sput ctx topic payload
  case r of
    Left errmsg   -> do
      Colog.logError $ "Database error: " <> T.pack errmsg
      let resp = mkSPutResp topic 0 False
      Colog.logDebug $ T.pack ("Sending: " ++ show resp)
      HESP.sendMsg sock resp
    Right entryID -> do
      let resp = mkSPutResp topic entryID True
      Colog.logDebug $ T.pack ("Sending: " ++ show resp)
      HESP.sendMsg sock resp
  return $ Just ()
processSPut _ _ _ = return Nothing

processSGet :: TCP.Socket -> Context -> RequestType -> App (Maybe ())
processSGet sock ctx (SGet topic sid eid maxn offset) = do
  Colog.logInfo $ "Reading " <> decodeUtf8 topic <> " ..."
  r <- liftIO $ Store.sget ctx topic sid eid offset maxn
  case r of
    Left errmsg -> do
      Colog.logError $ "Database error: " <> T.pack errmsg
      let resp = mkSGetResp topic [] False
      Colog.logDebug $ "Sending: " <> (T.pack . show) resp
      HESP.sendMsg sock resp
    Right xs    -> do
      let resp = mkSGetResp topic xs True
      Colog.logDebug $ "Sending: " <> (T.pack . show) resp
      HESP.sendMsg sock resp
  return $ Just ()
processSGet _ _ _ = return Nothing

-------------------------------------------------------------------------------
-- Messages send to client

mkSPutResp :: ByteString
           -> Word64
           -> Bool
           -> HESP.Message
mkSPutResp topic entryID res =
  let status = if res then "OK" else "ERR"
      fin    = if res then encodeUtf8 . T.pack . show $ entryID
                      else "Message storing failed."
  in HESP.mkPushFromList "sput" [ HESP.mkBulkString topic
                                , HESP.mkBulkString status
                                , HESP.mkBulkString fin
                                ]

mkSGetResp :: ByteString
           -> [(ByteString, Word64)]
           -> Bool
           -> HESP.Message
mkSGetResp topic contents res =
  let status = if res then "OK" else "ERR"
      fin    = if res then HESP.mkArrayFromList
        ((\(p,i) -> HESP.mkArrayFromList [ HESP.mkBulkString (BSC.pack . show $ i)
                                         , HESP.mkBulkString p
                                         ]) <$> contents)
                      else HESP.mkBulkString "Message fetching failed"
  in HESP.mkPushFromList "sget" [ HESP.mkBulkString topic
                                , HESP.mkBulkString status
                                , fin
                                ]
