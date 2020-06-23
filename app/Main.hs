{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import qualified Colog
import           Control.Concurrent.STM (newTVarIO)
import           Control.Exception      (bracket)
import           Control.Monad          (unless, when)
import           Control.Monad.Reader   (ask, liftIO, runReaderT)
import qualified Data.Map.Strict        as Map
import qualified Data.Text              as Text
import qualified Data.Vector            as V
import           Data.Yaml.Config       (loadYamlSettingsArgs, useEnv)
import qualified Network.HESP           as HESP
import qualified Network.Simple.TCP     as TCP
import           Network.Socket         (Socket)

import qualified Log.Store.Base         as LogStore
import           Mangrove               (App, Env (..), ServerSettings (..),
                                         ServerStatus (..),
                                         deleteOptionBySocket, onRecvMsg,
                                         runApp)

main :: IO ()
main = do
  settings@ServerSettings{..} <- loadYamlSettingsArgs [] useEnv
  options <- newTVarIO Map.empty
  let dbdir = dbPath
      env = Env { serverSettings = settings
                , serverStatus   = ServerStatus options
                }
  bracket
    (LogStore.initialize $ LogStore.UserDefinedEnv (LogStore.Config dbdir))
    (runReaderT LogStore.shutDown)
    (runApp env . runServer)

runServer :: LogStore.Context -> App ()
runServer ctx = do
  env@Env{..} <- ask
  let h = TCP.Host (serverHost serverSettings)
      p = show (serverPort serverSettings)
  Colog.logInfo "------------------------- Mangrove -------------------------"
  Colog.logInfo $ "Listening on "
    <> Text.pack (serverHost serverSettings) <> ":" <> Text.pack p
  TCP.serve h p $ \(sock, _) -> runApp env $ go sock
  where
    go :: Socket -> App ()
    go sock = do
      Env{..} <- ask
      msgs <- HESP.recvMsgs sock 1024
      unless (V.null msgs)
        (do Colog.logDebug $ "Received: " <> Text.pack (show msgs)
            mapM_ (onRecvMsg sock ctx) msgs
            go sock
        )
      when (V.null msgs)
        (liftIO $ deleteOptionBySocket sock serverStatus)
