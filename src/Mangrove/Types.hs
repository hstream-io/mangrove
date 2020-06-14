{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}

module Mangrove.Types
  ( QueryName
  , RequestID
  , RequestType (..)
  , ServerConfig (..)
  , Env (..)
  , App (..)

  , parseRequest
  , serverOpts
  ) where

import qualified Colog
import           Control.Monad.Reader  (MonadIO, MonadReader, ReaderT)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.Vector           as V
import           Data.Word             (Word64)
import qualified Network.HESP          as HESP
import           Network.HESP.Commands (commandParser, extractBulkStringParam,
                                        extractIntegerParam)
import           Options.Applicative   (Parser, ParserInfo, fullDesc, header,
                                        help, helper, info, long, metavar,
                                        progDesc, short, strOption, (<**>))
import           Text.Read             (readMaybe)

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

data ServerConfig = ServerConfig
    { serverPort :: String
    , dbPath     :: String
    }
    deriving (Show, Eq)

serverConfigP :: Parser ServerConfig
serverConfigP = ServerConfig
             <$> strOption
                 ( long "port"
                <> short 'p'
                <> metavar "PORT"
                <> help "Port that the server listening on" )
             <*> strOption
                 ( long "db-path"
                <> short 'd'
                <> metavar "DB-PATH"
                <> help "Path to the database on the disk" )

serverOpts :: ParserInfo ServerConfig
serverOpts = info (serverConfigP <**> helper)
  ( fullDesc
 <> progDesc "Process requests of database options"
 <> header "log-store-server - a simple database management server" )

data Env m = Env
    { envPort      :: !String
    , envDBPath    :: !String
    , envLogAction :: !(Colog.LogAction m Colog.Message)
    }

instance Colog.HasLog (Env m) Colog.Message m where
  getLogAction :: Env m -> Colog.LogAction m Colog.Message
  getLogAction = envLogAction
  {-# INLINE getLogAction #-}

  setLogAction :: Colog.LogAction m Colog.Message -> Env m -> Env m
  setLogAction newLogAction env = env { envLogAction = newLogAction }
  {-# INLINE setLogAction #-}

newtype App a = App
  { unApp :: ReaderT (Env App) IO a
  } deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader (Env App))
