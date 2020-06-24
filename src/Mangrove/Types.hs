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
  ( Env (..)
  , App
  , runApp

    -- * Server settings
  , ServerSettings (..)

    -- * Server status
  , ServerStatus
  , newServerStatus
    -- ** Client id
  , ClientId
  , newClientId
  , packClientId
  , packClientIdBS
  , getClientIdFromASCIIBytes
  , getClientIdFromASCIIBytes'
    -- ** Client options
  , ClientOptions (..)
  , getClientOptions
  , insertClientOptions
  , deleteClientOptions
  , deleteClientOptionsBy
  , deleteClientOptionsBySocket

    -- * Client requests
  , RequestType (..)
  ) where

import qualified Colog
import           Control.Applicative    ((<|>))
import           Control.DeepSeq        (NFData)
import           Control.Monad.IO.Class (MonadIO)
import           Control.Monad.Reader   (MonadReader, ReaderT, runReaderT)
import           Data.Aeson             (FromJSON (..), (.:))
import qualified Data.Aeson             as Aeson
import qualified Data.Aeson.Types       as Aeson
import           Data.ByteString        (ByteString)
import           Data.Hashable          (Hashable)
import           Data.HashMap.Strict    (HashMap)
import qualified Data.HashMap.Strict    as HMap
import           Data.Text              (Text)
import           Data.UUID              (UUID)
import qualified Data.UUID              as UUID
import qualified Data.UUID.V4           as UUID
import qualified Data.Vector            as V
import           Data.Word              (Word64)
import           GHC.Conc               (TVar, atomically, newTVarIO, readTVar,
                                         readTVarIO, writeTVar)
import           GHC.Generics           (Generic)
import           Network.Socket         (Socket)
import qualified Network.Socket         as NS

-------------------------------------------------------------------------------

data Env m =
  Env { serverSettings :: ServerSettings m
      , serverStatus   :: ServerStatus
      }

newtype App a = App { unApp :: ReaderT (Env App) IO a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader (Env App))

runApp :: Env App -> App a -> IO a
runApp env app = runReaderT (unApp app) env

-------------------------------------------------------------------------------
-- Server settings

data ServerSettings m =
  ServerSettings { serverHost    :: !NS.HostName
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

-------------------------------------------------------------------------------
-- Server status

newtype ClientId = ClientId { unClientId :: UUID }
  deriving newtype (Show, Eq, Read, Hashable, NFData)

newClientId :: IO ClientId
newClientId = ClientId <$> UUID.nextRandom

packClientId :: ClientId -> Text
packClientId = UUID.toText . unClientId

packClientIdBS :: ClientId -> ByteString
packClientIdBS = UUID.toASCIIBytes . unClientId

getClientIdFromASCIIBytes :: ByteString -> Maybe ClientId
getClientIdFromASCIIBytes = (fmap ClientId) . UUID.fromASCIIBytes

getClientIdFromASCIIBytes' :: ByteString -> Either ByteString ClientId
getClientIdFromASCIIBytes' bs =
  case getClientIdFromASCIIBytes bs of
    Just clientid -> Right clientid
    Nothing       -> Left $ "Invalid client-id: " <> bs <> "."

data ClientOptions =
  ClientOptions { clientSock     :: !Socket
                , clientPubLevel :: !Integer
                }

data ServerStatus =
  ServerStatus { clientSettings :: TVar (HashMap ClientId ClientOptions)
               }

newServerStatus :: IO ServerStatus
newServerStatus = do
  clientSettings <- newTVarIO HMap.empty
  return ServerStatus { clientSettings = clientSettings
                      }

getClientOptions :: ServerStatus -> ClientId -> IO (Maybe ClientOptions)
getClientOptions ServerStatus{..} cid = do
  settings <- readTVarIO clientSettings
  return $ HMap.lookup cid settings

insertClientOptions :: ServerStatus -> ClientId -> ClientOptions -> IO ()
insertClientOptions ServerStatus{..} cid options = atomically $ do
  s <- readTVar clientSettings
  writeTVar clientSettings $! (HMap.insert cid options s)

-- | Remove the 'ClientOptions' for the specified 'ClientId' from
-- 'ServerStatus' if present.
deleteClientOptions :: ServerStatus -> ClientId -> IO ()
deleteClientOptions ServerStatus{..} cid = atomically $ do
  s <- readTVar clientSettings
  writeTVar clientSettings $! (HMap.delete cid s)

deleteClientOptionsBy :: ServerStatus -> (ClientOptions -> Bool) -> IO ()
deleteClientOptionsBy ServerStatus{..} cond = atomically $ do
  s <- readTVar clientSettings
  writeTVar clientSettings $! HMap.filter (not . cond) s

deleteClientOptionsBySocket :: ServerStatus -> Socket -> IO ()
deleteClientOptionsBySocket status sock =
  deleteClientOptionsBy status $ \otps -> clientSock otps == sock

-------------------------------------------------------------------------------
-- Client requests

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
