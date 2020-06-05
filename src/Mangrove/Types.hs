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

  , recvReq
  , parseRequest
  , serverOpts
  ) where

import qualified Colog
import           Control.Monad.Reader  (MonadIO, MonadReader, ReaderT)
import           Data.ByteString       (ByteString, append)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.Vector           as V
import qualified Network.HESP          as HESP
import           Network.HESP.Commands (commandParser, extractBulkStringParam)
import           Options.Applicative   (Parser, ParserInfo, fullDesc, header,
                                        help, helper, info, long, metavar,
                                        progDesc, short, strOption, (<**>))

type QueryName = String
type RequestID = ByteString

data RequestType = SPut ByteString ByteString
    deriving (Show, Eq)

parseSPut :: V.Vector HESP.Message
          -> Either ByteString RequestType
parseSPut paras = do
  topic   <- extractBulkStringParam "Topic"      paras 0
  payload <- extractBulkStringParam "Payload"    paras 1
  return   $ SPut topic payload

parseRequest :: HESP.Message
             -> Either ByteString RequestType
parseRequest msg = do
  (n, paras) <- commandParser msg
  case n of
    "sput" -> parseSPut paras
    _      -> Left $ "Unrecognized request " <> n <> "."

recvReq :: Either String HESP.Message
        -> Either ByteString RequestType
recvReq (Left e)    = Left (BSC.pack e)
recvReq (Right msg) = parseRequest msg


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
