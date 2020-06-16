{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
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

  , QueryName
  , RequestID
  , RequestType (..)
  , parseRequest
  ) where

import qualified Colog
import           Control.Applicative    ((<|>))
import           Control.Monad.IO.Class (MonadIO)
import           Control.Monad.Reader   (MonadReader, ReaderT, runReaderT)
import           Data.Aeson             (FromJSON (..), (.:))
import qualified Data.Aeson             as Aeson
import qualified Data.Aeson.Types       as Aeson
import           Data.ByteString        (ByteString)
import qualified Data.ByteString.Char8  as BSC
import           Data.Text              (Text)
import qualified Data.Vector            as V
import           Data.Word              (Word64)
import           GHC.Generics           (Generic)
import qualified Network.HESP           as HESP
import           Network.HESP.Commands  (commandParser, extractBulkStringParam,
                                         extractIntegerParam)
import qualified Network.Socket         as NS
import           Text.Read              (readMaybe)

-------------------------------------------------------------------------------

data Env m =
  Env { serverHost    :: !NS.HostName
      , serverPort    :: !Int
      , dbPath        :: !String
      , loggerSetting :: !(LoggerSetting m)
      }
  deriving (Generic, FromJSON)

newtype LoggerSetting m =
  LoggerSetting { logAction :: Colog.LogAction m Colog.Message }

instance MonadIO m => FromJSON (LoggerSetting m) where
  parseJSON v = Aeson.withObject "logger"        customMode v
            <|> Aeson.withText   "simple-logger" simpleMode v

instance Colog.HasLog (Env m) Colog.Message m where
  getLogAction :: Env m -> Colog.LogAction m Colog.Message
  getLogAction = logAction . loggerSetting
  {-# INLINE getLogAction #-}

  setLogAction :: Colog.LogAction m Colog.Message -> Env m -> Env m
  setLogAction new env = env { loggerSetting = LoggerSetting new }
  {-# INLINE setLogAction #-}

newtype App a = App { unApp :: ReaderT (Env App) IO a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader (Env App))

runApp :: Env App -> App a -> IO a
runApp env app = runReaderT (unApp app) env

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
-- Parse client requests

type QueryName = String
type RequestID = ByteString

data RequestType = SPut ByteString ByteString
    | SGet ByteString (Maybe Word64) (Maybe Word64) Integer Integer
    deriving (Show, Eq)

parseSPut :: V.Vector HESP.Message
          -> Either ByteString RequestType
parseSPut paras = do
  topic   <- extractBulkStringParam "Topic"      paras 0
  payload <- extractBulkStringParam "Payload"    paras 1
  return   $ SPut topic payload

validateInt :: ByteString -> ByteString -> Either ByteString (Maybe Word64)
validateInt label s
  | s == ""   = Right Nothing
  | otherwise = case readMaybe (BSC.unpack s) of
      Nothing -> Left $ label <> " must be an integer."
      Just x  -> Right (Just x)

parseSGet :: V.Vector HESP.Message
          -> Either ByteString RequestType
parseSGet paras = do
  topic   <- extractBulkStringParam "Topic"              paras 0
  sids    <- extractBulkStringParam "Start ID"           paras 1
  sid     <- validateInt            "Start ID"           sids
  eids    <- extractBulkStringParam "End ID"             paras 2
  eid     <- validateInt            "End ID"             eids
  maxn    <- extractIntegerParam    "Max message number" paras 3
  offset  <- extractIntegerParam    "Offset"             paras 4
  return $ SGet topic sid eid maxn offset

parseRequest :: HESP.Message
             -> Either ByteString RequestType
parseRequest msg = do
  (n, paras) <- commandParser msg
  case n of
    "sput" -> parseSPut paras
    "sget" -> parseSGet paras
    _      -> Left $ "Unrecognized request " <> n <> "."
