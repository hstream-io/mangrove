{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RecordWildCards            #-}

module Main where

import           Colog                 (HasLog, LogAction, Message,
                                        getLogAction, logError, logInfo,
                                        richMessageAction, setLogAction)
import           Control.Concurrent    (threadDelay)
import           Control.Monad         (forever)
import           Control.Monad.Catch   (MonadCatch, MonadMask, MonadThrow)
import           Control.Monad.Reader  (MonadIO, MonadReader, ReaderT, ask,
                                        liftIO, runReaderT)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.Text             as T
import           Data.Time             (getCurrentTime)
import qualified Network.HESP          as HESP
import qualified Network.Simple.TCP    as TCP
import           Options.Applicative   (Parser, ParserInfo, execParser,
                                        fullDesc, header, help, helper, info,
                                        long, metavar, progDesc, short,
                                        strOption, (<**>))

data ClientConfig = ClientConfig
    { toHost :: String
    , toPort :: String
    }
    deriving (Show, Eq)

clientConfigP :: Parser ClientConfig
clientConfigP = ClientConfig
             <$> strOption
                 ( long "host"
                <> short 'h'
                <> metavar "HOST"
                <> help "Host address to connect to" )
             <*> strOption
                 ( long "port"
                <> short 'p'
                <> metavar "PORT"
                <> help "Port of server to connect to" )

clientOpts :: ParserInfo ClientConfig
clientOpts = info (clientConfigP <**> helper)
  ( fullDesc
 <> progDesc "Send database R/W requests to server"
 <> header "log-store-test - simple client for testing usage" )

data Env m = Env
    { envHost      :: !String
    , envPort      :: !String
    , envLogAction :: !(LogAction m Message)
    }

instance HasLog (Env m) Message m where
  getLogAction :: Env m -> LogAction m Message
  getLogAction = envLogAction
  {-# INLINE getLogAction #-}

  setLogAction :: LogAction m Message -> Env m -> Env m
  setLogAction newLogAction env = env { envLogAction = newLogAction }
  {-# INLINE setLogAction #-}

newtype App a = App
  { unApp :: ReaderT (Env App) IO a
  } deriving newtype ( Functor, Applicative, Monad, MonadIO, MonadReader (Env App)
                     , MonadMask, MonadCatch, MonadThrow)

app :: App ()
app = do
  Env{..} <- ask

  TCP.connect "0.0.0.0" "20202" $ \(s,_) -> forever $ do
    time <- liftIO getCurrentTime
    liftIO $ HESP.sendMsgs s $
      [
        HESP.mkArrayFromList [ HESP.mkBulkString "sput"
                             , HESP.mkBulkString "topic_1"
                             , HESP.mkBulkString (BSC.pack . show $ time)
                             ]
      , HESP.mkArrayFromList [ HESP.mkBulkString "sputt"
                             , HESP.mkBulkString "topic_2"
                             , HESP.mkBulkString (BSC.pack . show $ time)
                             ]
      ]
    liftIO $ threadDelay 1000000
    time' <- liftIO getCurrentTime
    liftIO $ HESP.sendMsg s $
      HESP.mkArrayFromList [ HESP.mkBulkString "sput"
                           , HESP.mkBulkString "00000"
                           , HESP.mkBulkString "topic_1"
                           , HESP.mkBulkString (BSC.pack . show $ time')
                           ]
    acks <- liftIO $ HESP.recvMsgs s 1024
    mapM_ processAck acks
    liftIO $ threadDelay 1000000
 where
   processAck :: Either String HESP.Message -> App ()
   processAck ack = case ack of
       Left s    -> logError (T.pack s)
       Right msg -> case msg of
         HESP.MatchSimpleError _ _ -> logError . T.pack . show $ HESP.serialize msg
         _                         -> logInfo  . T.pack . show $ HESP.serialize msg

runApp :: Env App -> App a -> IO a
runApp env app = runReaderT (unApp app) env


main :: IO ()
main = do
  ClientConfig{..} <- execParser clientOpts
  let env@Env{..} = Env toHost toPort richMessageAction
  runApp env app
