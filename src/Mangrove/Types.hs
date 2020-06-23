{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RecordWildCards            #-}

module Mangrove.Types
  ( ClientOption (..)
  , ServerSettings (..)
  , ServerStatus (..)
  , Env (..)
  , ClientId
  , App
  , runApp
  , getOption
  , insertOption
  , deleteOptionBySocket

  , QueryName
  , RequestID
  , RequestType (..)
  ) where

import qualified Colog
import           Control.Applicative    ((<|>))
import           Control.Concurrent.STM (TVar, atomically, modifyTVar,
                                         readTVarIO, writeTVar)
import           Control.Monad.IO.Class (MonadIO)
import           Control.Monad.Reader   (MonadReader, ReaderT, runReaderT)
import           Data.Aeson             (FromJSON (..), (.:))
import qualified Data.Aeson             as Aeson
import qualified Data.Aeson.Types       as Aeson
import           Data.ByteString        (ByteString)
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Text              (Text)
import           Data.UUID              (UUID)
import qualified Data.Vector            as V
import           Data.Word              (Word64)
import           GHC.Generics           (Generic)
import qualified Network.Socket         as NS

-------------------------------------------------------------------------------

data ClientOption = ClientOption
    { clientSock     :: !NS.Socket
    , clientPubLevel :: !Integer
    }

data ServerSettings m = ServerSettings
    { serverHost    :: !NS.HostName
    , serverPort    :: !Int
    , dbPath        :: !String
    , loggerSetting :: !(LoggerSetting m)
    }
    deriving (Generic)

instance MonadIO m => FromJSON (ServerSettings m) where
  parseJSON =
    let opts = Aeson.defaultOptions { Aeson.fieldLabelModifier = flm }
        flm "serverHost"    = "host"
        flm "serverPort"    = "port"
        flm "dbPath"        = "db-path"
        flm "loggerSetting" = "logger"
        flm x               = x
     in Aeson.genericParseJSON opts

type ClientId = UUID

data ServerStatus = ServerStatus
    { clientOptions :: TVar (Map ClientId ClientOption)
    }

getOption :: ClientId -> ServerStatus -> IO (Maybe ClientOption)
getOption cid ServerStatus{..} = do
  options <- readTVarIO clientOptions
  return $ (Map.!?) options cid

insertOption :: ClientId -> ClientOption -> ServerStatus -> IO ()
insertOption cid option ServerStatus{..} =
  atomically $ modifyTVar clientOptions (Map.insert cid option)

deleteOptionBySocket :: NS.Socket -> ServerStatus -> IO ()
deleteOptionBySocket sock ServerStatus{..} = do
  options <- readTVarIO clientOptions
  let options' = Map.filter (\option -> clientSock option /= sock) options
  atomically $ writeTVar clientOptions options'

data Env m = Env
    { serverSettings :: ServerSettings m
    , serverStatus   :: ServerStatus
    }

newtype LoggerSetting m =
  LoggerSetting { logAction :: Colog.LogAction m Colog.Message }

instance MonadIO m => FromJSON (LoggerSetting m) where
  parseJSON v = Aeson.withObject "logger"        customMode v
            <|> Aeson.withText   "simple-logger" simpleMode v

instance Colog.HasLog (Env m) Colog.Message m where
  getLogAction :: Env m -> Colog.LogAction m Colog.Message
  getLogAction = logAction . loggerSetting . serverSettings
  {-# INLINE getLogAction #-}

  setLogAction :: Colog.LogAction m Colog.Message -> Env m -> Env m
  setLogAction new env =
    let settings = serverSettings env
     in env {serverSettings = settings {loggerSetting = LoggerSetting new}}
  {-# INLINE setLogAction #-}

newtype App a = App { unApp :: ReaderT (Env App) IO a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader (Env App))

runApp :: Env App -> App a -> IO a
runApp env app = runReaderT (unApp app) env

-------------------------------------------------------------------------------
-- Client requests

type QueryName = String
type RequestID = ByteString

data RequestType
  = Handshake Integer
  | SPut ClientId ByteString ByteString
  | SPuts ClientId ByteString (V.Vector ByteString)
  | SGet ByteString (Maybe Word64) (Maybe Word64) Integer Integer
  deriving (Show, Eq)

-------------------------------------------------------------------------------
-- Logger Settings

simpleMode :: MonadIO m => Text -> Aeson.Parser (LoggerSetting m)
simpleMode fmt = LoggerSetting <$> formatter fmt

customMode :: MonadIO m => Aeson.Object -> Aeson.Parser (LoggerSetting m)
customMode obj = do
  fmt <- obj .: "formatter" :: Aeson.Parser Text
  lvl <- obj .: "level"     :: Aeson.Parser Text
  action <- formatter fmt
  LoggerSetting <$> level action lvl

formatter :: MonadIO m => Text -> Aeson.Parser (Colog.LogAction m Colog.Message)
formatter "rich"   = return Colog.richMessageAction
formatter "simple" = return Colog.simpleMessageAction
formatter _        = fail "Invalid logger formatter"

level :: MonadIO m
      => Colog.LogAction m Colog.Message
      -> Text
      -> Aeson.Parser (Colog.LogAction m Colog.Message)
level action = \case
  "debug"   -> return $ Colog.filterBySeverity Colog.D sev action
  "info"    -> return $ Colog.filterBySeverity Colog.I sev action
  "warning" -> return $ Colog.filterBySeverity Colog.W sev action
  "error"   -> return $ Colog.filterBySeverity Colog.E sev action
  _         -> fail "Invalid logger level"
  where sev Colog.Msg{..} = msgSeverity
