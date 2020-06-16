{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import qualified Colog
import           Control.Exception    (bracket)
import           Control.Monad        (unless)
import           Control.Monad.Reader (ask, runReaderT)
import qualified Data.Text            as Text
import qualified Data.Vector          as V
import           Data.Yaml.Config     (loadYamlSettingsArgs, useEnv)
import qualified Network.HESP         as HESP
import qualified Network.Simple.TCP   as TCP
import           Network.Socket       (Socket)

import qualified Log.Store.Base       as LogStore
import           Mangrove             (App, Env (..), processMsg, runApp)

main :: IO ()
main = do
  env <- loadYamlSettingsArgs [] useEnv
  let dbdir = dbPath env
  bracket
    (LogStore.initialize $ LogStore.UserDefinedEnv (LogStore.Config dbdir))
    (runReaderT LogStore.shutDown)
    (runApp env . runServer)

runServer :: LogStore.Context -> App ()
runServer ctx = do
  env@Env{..} <- ask
  let h = TCP.Host serverHost
      p = show serverPort
  Colog.logInfo "------------------------- Mangrove -------------------------"
  Colog.logInfo $ "Listening on " <> Text.pack serverHost <> ":" <> Text.pack p
  TCP.serve h p $ \(sock, _) -> runApp env $ go sock
  where
    go :: Socket -> App ()
    go sock = do
      msgs <- HESP.recvMsgs sock 1024
      unless (V.null msgs)
        (do Colog.logDebug $ "Received: " <> Text.pack (show msgs)
            mapM_ (processMsg sock ctx) msgs
            go sock
        )
