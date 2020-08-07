{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE TypeFamilies               #-}

module Mangrove.Types
  ( Env (..)
  , App
  , runApp

  , Element

    -- * Server
    -- ** Server settings
  , ServerSettings (..)
  , DatabaseSetting (..)
    -- * Server status
  , ServerStatus
  , newServerStatus
  , getClient
  , insertClient
  , deleteClient
  , deleteClientsBy
  , deleteClientsBySocket

    -- * Client
    -- ** Client id
  , ClientId
  , newClientId
  , packClientId
  , packClientIdBS
  , getClientIdFromASCIIBytes
  , getClientIdFromASCIIBytes'
    -- ** Client status
  , ClientStatus
  , newClientStatus
  , clientSocket
  , getClientOption
  , extractClientPubLevel
  , extractClientPubMethod
  , extractClientSubLevel
    -- ** Consumer
  , insertConsumeStream
  , tryInsertConsumeStream
  , deleteClientConsume
  , takeConsumeElements
  , withConsumeElements

    -- * Client requests
  , RequestType (..)
  ) where

import qualified Colog
import           Control.Applicative         ((<|>))
import qualified Control.Concurrent.MVar     as Conc
import           Control.DeepSeq             (NFData)
import           Control.Monad.Base          (MonadBase)
import           Control.Monad.IO.Class      (MonadIO)
import           Control.Monad.Reader        (MonadReader, ReaderT, runReaderT)
import           Control.Monad.Trans.Control (MonadBaseControl (..))
import           Data.Aeson                  (FromJSON (..), (.:))
import qualified Data.Aeson                  as Aeson
import qualified Data.Aeson.Types            as Aeson
import           Data.ByteString             (ByteString)
import           Data.Hashable               (Hashable)
import           Data.HashMap.Strict         (HashMap)
import qualified Data.HashMap.Strict         as HMap
import           Data.Map.Strict             (Map)
import qualified Data.Map.Strict             as Map
import           Data.Text                   (Text)
import           Data.UUID                   (UUID)
import qualified Data.UUID                   as UUID
import qualified Data.UUID.V4                as UUID
import qualified Data.Vector                 as V
import           Data.Word                   (Word64, Word32)
import qualified GHC.Conc                    as Conc
import           GHC.Generics                (Generic)
import           Network.Socket              (Socket)
import qualified Network.Socket              as NS

import           Log.Store.Base              (Entry, EntryID)
import           Mangrove.Streaming          (Serial)
import qualified Mangrove.Streaming          as S
import qualified Network.HESP                as HESP
import qualified Network.HESP.Commands       as HESP

-------------------------------------------------------------------------------

type Topic = ByteString
type Element = (Entry, EntryID)

data Env m =
  Env { serverSettings :: ServerSettings m
      , serverStatus   :: ServerStatus
      }

newtype App a = App { unApp :: ReaderT (Env App) IO a }
  deriving newtype ( Functor, Applicative, Monad
                   , MonadIO, MonadReader (Env App), MonadBase IO
                   )

instance MonadBaseControl IO App where
  type StM App a = a
  liftBaseWith f = App $ liftBaseWith $ \x -> f (x . unApp)
  restoreM = App . restoreM

runApp :: Env App -> App a -> IO a
runApp env app = runReaderT (unApp app) env

-------------------------------------------------------------------------------
-- Server settings

data ServerSettings m =
  ServerSettings { serverHost      :: !NS.HostName
                 , serverPort      :: !Int
                 , databaseSetting :: !DatabaseSetting
                 , loggerSetting   :: !(LoggerSetting m)
                 }
  deriving (Generic)

data DatabaseSetting =
  DatabaseSetting { dbPath          :: !String
                  , writeBufferSize :: !Word64
                  , enableDBStats   :: !Bool
                  , statsPeriodSec  :: !Word32
                  }
  deriving (Generic)

instance MonadIO m => FromJSON (ServerSettings m) where
  parseJSON =
    let opts = Aeson.defaultOptions { Aeson.fieldLabelModifier = flm }
        flm "serverHost"      = "host"
        flm "serverPort"      = "port"
        flm "loggerSetting"   = "logger"
        flm "databaseSetting" = "database"
        flm x                 = x
     in Aeson.genericParseJSON opts

instance FromJSON DatabaseSetting where
  parseJSON =
    let opts = Aeson.defaultOptions { Aeson.fieldLabelModifier = flm }
        flm "dbPath"          = "path"
        flm "writeBufferSize" = "write-buffer-size"
        flm "enableDBStats"   = "enable-stats"
        flm "statsPeriodSec"  = "stats-period"
        flm x                 = x
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

data ServerStatus =
  ServerStatus { serverClients   :: !(Conc.TVar (HashMap ClientId ClientStatus))
               }

newServerStatus :: IO ServerStatus
newServerStatus = do
  clients <- Conc.newTVarIO HMap.empty
  return ServerStatus { serverClients   = clients
                      }

getClient :: ClientId -> ServerStatus -> IO (Maybe ClientStatus)
getClient cid ServerStatus{..} = do
  settings <- Conc.readTVarIO serverClients
  return $ HMap.lookup cid settings

insertClient :: ClientId -> ClientStatus -> ServerStatus -> IO ()
insertClient cid options ServerStatus{..} = Conc.atomically $ do
  s <- Conc.readTVar serverClients
  Conc.writeTVar serverClients $! HMap.insert cid options s

-- | Remove the client for the specified 'ClientId' from
-- 'ServerStatus' if present.
deleteClient :: ClientId -> ServerStatus -> IO ()
deleteClient cid ServerStatus{..} = Conc.atomically $ do
  s <- Conc.readTVar serverClients
  Conc.writeTVar serverClients $! HMap.delete cid s

deleteClientsBy :: (ClientStatus -> Bool) -> ServerStatus -> IO ()
deleteClientsBy cond ServerStatus{..} = Conc.atomically $ do
  s <- Conc.readTVar serverClients
  Conc.writeTVar serverClients $! HMap.filter (not . cond) s

deleteClientsBySocket :: Socket -> ServerStatus -> IO ()
deleteClientsBySocket sock = deleteClientsBy (\otps -> clientSock otps == sock)

-------------------------------------------------------------------------------
-- Clients

newtype ClientId = ClientId { unClientId :: UUID }
  deriving newtype (Show, Eq, Read, Hashable, NFData)

newClientId :: IO ClientId
newClientId = ClientId <$> UUID.nextRandom

packClientId :: ClientId -> Text
packClientId = UUID.toText . unClientId

packClientIdBS :: ClientId -> ByteString
packClientIdBS = UUID.toASCIIBytes . unClientId

getClientIdFromASCIIBytes :: ByteString -> Maybe ClientId
getClientIdFromASCIIBytes = fmap ClientId . UUID.fromASCIIBytes

getClientIdFromASCIIBytes' :: ByteString -> Either ByteString ClientId
getClientIdFromASCIIBytes' bs =
  case getClientIdFromASCIIBytes bs of
    Just clientid -> Right clientid
    Nothing       -> Left $ "Invalid client-id: " <> bs <> "."

data ClientStatus =
  ClientStatus { clientSock    :: !Socket
               , clientOptions :: !(Map HESP.Message HESP.Message)
               , consumeStream :: !(Conc.MVar (HashMap Topic (Serial Element)))
               }

-- | Create client status from connection and handshake message
-- sent through hesp.
newClientStatus :: Socket -> Map HESP.Message HESP.Message -> IO ClientStatus
newClientStatus sock opts = do
  svar <- Conc.newMVar HMap.empty
  return ClientStatus { clientSock    = sock
                      , clientOptions = opts
                      , consumeStream = svar
                      }

clientSocket :: ClientStatus -> Socket
clientSocket = clientSock

getClientOption :: HESP.Message -> ClientStatus -> Maybe HESP.Message
getClientOption key ClientStatus{ clientOptions = opts } = Map.lookup key opts

extractClientPubLevel :: ClientStatus -> Either ByteString Integer
extractClientPubLevel = extractClientIntOptions "pub-level"

extractClientPubMethod :: ClientStatus -> Either ByteString Integer
extractClientPubMethod = extractClientIntOptions "pub-method"

extractClientSubLevel :: ClientStatus -> Either ByteString Integer
extractClientSubLevel = extractClientIntOptions "sub-level"

insertConsumeStream :: Topic
                    -> Serial Element
                    -> ClientStatus
                    -> IO ()
insertConsumeStream topic s client =
  Conc.modifyMVar_ (consumeStream client) $ \table ->
    return $ HMap.insert topic s table

tryInsertConsumeStream :: Topic
                       -> Serial Element
                       -> ClientStatus
                       -> IO Bool
tryInsertConsumeStream topic s client =
  Conc.modifyMVar (consumeStream client) $ \table ->
    if HMap.member topic table then return (table, False)
                               else return (HMap.insert topic s table, True)

deleteClientConsume :: Topic -> ClientStatus -> IO ()
deleteClientConsume topic client =
  Conc.modifyMVar_ (consumeStream client) $ \table ->
    return $ HMap.delete topic table

takeConsumeElements :: ClientStatus
                    -> Topic
                    -> Int
                    -> IO (Maybe (Serial Element))
takeConsumeElements client topic maxn =
  Conc.modifyMVar (consumeStream client) $ \table -> do
    let ms = HMap.lookup topic table
    case ms of
      Nothing -> return (table, Nothing)
      Just es -> let (xs, ys) = S.splitAt maxn es
                     !remain = HMap.adjust (const ys) topic table
                  in return (remain, Just xs)

withConsumeElements :: ClientStatus
                    -> Topic
                    -> Int
                    -> (Maybe (Serial Element) -> IO ())
                    -> IO ()
withConsumeElements client topic maxn f =
  Conc.modifyMVar_ (consumeStream client) $ \table -> do
    let ms = HMap.lookup topic table
    case ms of
      Nothing -> f ms >> return table
      Just es -> let (xs, ys) = S.splitAt maxn es
                  in f (Just xs) >> return (HMap.adjust (const ys) topic table)

-------------------------------------------------------------------------------
-- Client requests

data RequestType
  = Handshake (Map HESP.Message HESP.Message)
  | SPut ClientId ByteString ByteString
  | SPuts ClientId ByteString (V.Vector ByteString)
  | SGet ClientId ByteString (Maybe Word64) (Maybe Word64) Integer
  | SGetCtrl ClientId ByteString Integer
  | SRange ClientId ByteString (Maybe Word64) (Maybe Word64) Integer Integer
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

-------------------------------------------------------------------------------

validateInteger :: HESP.Message -> ByteString -> Either ByteString Integer
validateInteger (HESP.Integer x) _ = Right x
validateInteger _ label            = Left $ label <> " must be an integer."

extractClientIntOptions :: ByteString
                        -> ClientStatus
                        -> Either ByteString Integer
extractClientIntOptions label ClientStatus{ clientOptions = opts } = do
  value <- HESP.extractMapField opts (HESP.mkBulkString label)
  validateInteger value label
