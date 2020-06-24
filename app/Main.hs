{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import qualified Colog
import           Control.Exception    (bracket)
import           Control.Monad.Reader (ask, liftIO, runReaderT)
import qualified Data.Text            as Text
import qualified Data.Vector          as V
import           Data.Yaml.Config     (loadYamlSettingsArgs, useEnv)
import qualified Network.HESP         as HESP
import qualified Network.Simple.TCP   as TCP
import           Network.Socket       (Socket)

import qualified Log.Store.Base       as LogStore
import           Mangrove             (App, Env (..), ServerSettings (..))
import qualified Mangrove             as Mangrove

main :: IO ()
main = do
  settings@ServerSettings{..} <- loadYamlSettingsArgs [] useEnv
  status <- Mangrove.newServerStatus
  let env = Env { serverSettings = settings
                , serverStatus   = status
                }
  bracket
    (LogStore.initialize $ LogStore.UserDefinedEnv (LogStore.Config dbPath))
    (runReaderT LogStore.shutDown)
    (Mangrove.runApp env . runServer)

runServer :: LogStore.Context -> App ()
runServer ctx = do
  env@Env{..} <- ask
  let h = TCP.Host (serverHost serverSettings)
      p = show (serverPort serverSettings)
  Colog.logInfo "------------------------- Mangrove -------------------------"
  Colog.logInfo $ "Listening on "
    <> Text.pack (serverHost serverSettings) <> ":" <> Text.pack p
  TCP.serve h p $ \(sock, _) -> Mangrove.runApp env $ go sock
  where
    go :: Socket -> App ()
    go sock = do
      env <- ask
      msgs <- HESP.recvMsgs sock 1024
      if V.null msgs
         then do
           let status = serverStatus env
           liftIO $ Mangrove.deleteClientOptionsBySocket status sock
         else do
           Colog.logDebug $ "Received: " <> Text.pack (show msgs)
           mapM_ (Mangrove.onRecvMsg sock ctx) msgs
           go sock
