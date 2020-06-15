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
import qualified Network.Socket        as NS

import qualified Mangrove.Store        as Store
import           Mangrove.Types        (App (..), Env (..), RequestType (..),
                                        parseRequest)
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
    Right req -> processSPut  sock ctx req
             .|. processSPuts sock ctx req
             .|. processSGet  sock ctx req

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

processSPuts :: TCP.Socket -> Context -> RequestType -> App (Maybe ())
processSPuts sock ctx (SPuts topic payloads) = do
  Colog.logInfo $ "Writing " <> decodeUtf8 topic <> " ..."
  rs <- liftIO $ mapM (Store.sput ctx topic) payloads
  case sequence rs of
    Left errmsg    -> do
      Colog.logError $ "Database error: " <> T.pack errmsg
      let resp = mkSPutsResp topic (V.singleton 0) False
      Colog.logDebug $ T.pack ("Sending: " ++ show resp)
      HESP.sendMsg sock resp
    Right entryIDs -> do
      let resp = mkSPutsResp topic entryIDs True
      Colog.logDebug $ T.pack ("Sending: " ++ show resp)
      HESP.sendMsg sock resp
  return $ Just ()
processSPuts _ _ _ = return Nothing

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

mkSPutsResp :: ByteString
            -> V.Vector Word64
            -> Bool
            -> HESP.Message
mkSPutsResp topic entryIDs res =
  let status = if res then "OK" else "ERR"
      fin    = if res then HESP.mkArray
                           $ (HESP.mkBulkString . encodeUtf8 . T.pack . show) <$> entryIDs
                      else HESP.mkBulkString "Message storing failed."
   in HESP.mkPushFromList "sput" [ HESP.mkBulkString topic
                                 , HESP.mkBulkString status
                                 , fin
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
