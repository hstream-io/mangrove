{-# LANGUAGE DerivingStrategies    #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}

module Mangrove.Server
  ( runServer
  ) where

import qualified Colog
import           Control.Exception     (bracket)
import           Control.Monad         (unless)
import           Control.Monad.Reader  (liftIO, runReaderT)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.Text             as T
import           Data.Text.Encoding    (decodeUtf8, encodeUtf8)
import qualified Data.Vector           as V
import           Data.Word             (Word64)
import           Log.Store.Base        hiding (Env)
import qualified Network.HESP          as HESP
import qualified Network.Simple.TCP    as TCP
import           Network.Socket        (Socket)

import qualified Mangrove.Store        as Store
import           Mangrove.Types        (App (..), Env (..), RequestType (..),
                                        recvReq)
import           Mangrove.Utils        ((.|.))

runServer :: Env App -> IO a
runServer env@Env{..} =
    bracket
      (initialize $ UserDefinedEnv (Config envDBPath))
      (runReaderT shutDown)
      serverProcess
  where
    serverProcess ctx =
      TCP.serve (TCP.Host "0.0.0.0") envPort $ \(sock, _) ->
        runApp env $ go sock ctx
    go :: Socket -> Context -> App ()
    go sock ctx = do
      msgs' <- HESP.recvMsgs sock 1024
      unless (V.null msgs')
        (do Colog.logDebug $ "Received: " <> T.pack (show msgs')
            mapM_ (processMsg sock ctx) msgs'
            go sock ctx
        )

runApp :: Env App -> App a -> IO a
runApp env app = runReaderT (unApp app) env

-------------------------------------------------------------------------------

processMsg :: Socket
           -> Context
           -> Either String HESP.Message
           -> App (Maybe RequestType)
processMsg sock ctx msg =
  case recvReq msg of
    Left e    -> do
      HESP.sendMsg sock $ HESP.mkSimpleError "ERR" e
      -- FIXME: should we close this connection?
      Colog.logError $ "Failed to parse message: " <> decodeUtf8 e
      return Nothing
    Right req -> processSPut sock ctx req .|. processSGet sock ctx req

processSPut :: TCP.Socket -> Context -> RequestType -> App (Maybe RequestType)
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
  return Nothing
processSPut _ _ req = return $ Just req

processSGet :: TCP.Socket -> Context -> RequestType -> App (Maybe RequestType)
processSGet sock ctx (SGet topic sid eid maxn offset) = do
  Colog.logInfo $ "Reading " <> decodeUtf8 topic <> " ..."
  -- TODO: pass Nothing if sid/eid is null
  r <- liftIO $ Store.sget ctx topic (Just sid) (Just eid) offset maxn
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
  return Nothing
processSGet _ _ req = return $ Just req

-------------------------------------------------------------------------------

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
