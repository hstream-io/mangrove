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
import           Conduit                   (runConduit, sinkList, (.|))
import           Control.Exception         (bracket)
import           Control.Monad             (forever, unless)
import           Control.Monad.Reader      (liftIO, runReaderT)
import           Control.Monad.Trans.Maybe (runMaybeT)
import           Data.ByteString           (ByteString)
import qualified Data.ByteString.Char8     as BSC
import qualified Data.List                 as L
import           Data.Maybe                (fromJust, isJust)
import qualified Data.Text                 as T
import           Data.Text.Encoding        (encodeUtf8)
import qualified Data.Vector               as V
import           Data.Word                 (Word64)
import           Log.Store.Base            hiding (Env)
import           Mangrove.Types            (App (..), Env (..),
                                            RequestType (..), recvReq)
import qualified Network.HESP              as HESP
import qualified Network.Simple.TCP        as TCP

mkLog :: Env App -> Colog.Severity -> T.Text -> IO ()
mkLog env sev msg = runApp env (Colog.log sev msg)

runApp :: Env App -> App a -> IO a
runApp env app = runReaderT (unApp app) env

runServer :: Env App -> IO a
runServer env@Env{..} =
    bracket
      (initialize $ UserDefinedEnv (Config envDBPath))
      (runReaderT shutDown)
      serverProcess
  where
    serverProcess ctx = TCP.serve (TCP.Host "0.0.0.0") envPort $ \(s,_) -> do
      mkLog env Colog.I "Connected."
      go s ctx

    go s ctx = do
      msgs' <- HESP.recvMsgs s 1024
      unless (V.null msgs') (do
        mkLog env Colog.I $ T.pack ("Received: " ++ show msgs')
        mapM_ (processMsg' s ctx) msgs'
        go s ctx
        )

    processMsg' s ctx msg' = case recvReq msg' of
      Left e -> do
        HESP.sendMsg s $ HESP.mkSimpleError "ERR" e
        mkLog env Colog.W $ T.pack ("Failed to parse message: "
                            ++ show msg' ++ " (" ++ BSC.unpack e ++ ")")
      Right req@(SPut topic payload)             -> processSPut env ctx req s
      Right req@(SGet topic sid eid maxn offset) -> processSGet env ctx req s


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
                                , HESP.mkBulkString fin ]

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
                                , fin ]

processSPut :: Env App -> Context -> RequestType -> TCP.Socket -> IO ()
processSPut env@Env{..} ctx (SPut topic payload) s = do
  entryID' <- runMaybeT $ runReaderT (do
    lh <- open (BSC.unpack topic) defaultOpenOptions {writeMode=True, createIfMissing=True}
    liftIO $ mkLog env Colog.I $ T.pack ("Writing " ++ BSC.unpack topic ++ " ...")
    appendEntry lh payload
    ) ctx
  case entryID' of
    Nothing      -> do
      let resp = mkSPutResp topic 0 False
      HESP.sendMsg s resp
      mkLog env Colog.W $ T.pack ("Sent: " ++ show resp)
    Just entryID -> do
      let resp = mkSPutResp topic entryID True
      HESP.sendMsg s resp
      mkLog env Colog.I $ T.pack ("Sent: " ++ show resp)
processSPut _ _ _ _ = return ()

processSGet :: Env App -> Context -> RequestType -> TCP.Socket -> IO ()
processSGet env@Env{..} ctx (SGet topic sid eid maxn offset) s = do
  source' <- runMaybeT $ runReaderT (do
    lh <- open (BSC.unpack topic) defaultOpenOptions {writeMode=True, createIfMissing=True}
    liftIO $ mkLog env Colog.I $ T.pack ("Reading " ++ BSC.unpack topic ++ " ...")
    readEntries lh (Just sid) (Just eid)
    ) ctx
  case source' of
    Nothing -> do
      let resp = mkSGetResp topic [] False
      HESP.sendMsg s resp
      mkLog env Colog.W $ T.pack ("Sent: " ++ show resp)
    Just source -> do
      contents' <- runConduit $ source .| sinkList
      let contentsN' = L.take (fromIntegral maxn) $ L.drop (fromIntegral offset) contents'
          contentsN  = L.takeWhile isJust contentsN'
          ok = L.all isJust contentsN'
          resp = mkSGetResp topic (fromJust <$> contentsN) True
      HESP.sendMsg s resp
      mkLog env Colog.I $ T.pack ("Sent: " ++ show resp)
      unless ok $ do
        let resp' = mkSGetResp topic [] False
        HESP.sendMsg s resp'
        mkLog env Colog.E $ T.pack ("Sent: " ++ show resp')
        mkLog env Colog.E $ T.pack "Database error, thread terminated."
        error ""
processSGet _ _ _ _ = return ()
