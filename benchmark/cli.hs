{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main (main) where

import           Control.Applicative   ((<**>), (<|>))
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as BS
import           Data.Char             (ord)
import           Data.Maybe            (fromJust)
import qualified Data.Text             as Text
import qualified Data.Text.Encoding    as Text
import           Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector           as V
import qualified Network.HESP          as HESP
import qualified Network.HESP.Commands as HESP
import           Network.Socket        (HostName, Socket)
import           Numeric               (showFFloat)
import           Options.Applicative   (Parser)
import qualified Options.Applicative   as O
import           System.IO             (hPutStr, hSetBuffering)
import qualified System.IO             as SIO

-------------------------------------------------------------------------------

data App =
  App { connection :: ConnectionSetting
      , command    :: Command
      }

appOpts :: Parser App
appOpts = App <$> connectionOtpion <*> commandOtpion

main :: IO ()
main = do
  hSetBuffering SIO.stdout SIO.NoBuffering
  App{..} <- O.execParser $ O.info (appOpts <**> O.helper) O.fullDesc
  case command of
    SputCommand opts -> runTCPClient connection $ flip sput opts

runTCPClient :: ConnectionSetting -> (Socket -> IO a) -> IO a
runTCPClient ConnectionSetting{..} f =
  HESP.connect serverHost (show serverPort) $ \(sock, _) -> f sock

-------------------------------------------------------------------------------

data ConnectionSetting =
  ConnectionSetting { serverHost :: HostName
                    , serverPort :: Int
                    }

connectionOtpion :: Parser ConnectionSetting
connectionOtpion =
  ConnectionSetting
    <$> O.strOption (O.long "host"
                  <> O.short 'h'
                  <> O.showDefault
                  <> O.value "localhost"
                  <> O.metavar "HOST"
                  <> O.help "Server host")
    <*> O.option O.auto (O.long "port"
                      <> O.short 'p'
                      <> O.showDefault
                      <> O.value 20202
                      <> O.metavar "PORT"
                      <> O.help "Server port")

data Command = SputCommand SputOptions

commandOtpion :: Parser Command
commandOtpion = SputCommand <$> O.hsubparser sputCommand
  where
    sputCommand = O.command "sput" (O.info sputOptions (O.progDesc "SPUT -- publish messages to stream"))

data SputOptions =
  SputOptions { numOfBytes :: Int
              , topicName  :: TopicName
              , pubLevel   :: Int
              }

data TopicName = TopicName String | RandomTopicName

sputOptions :: Parser SputOptions
sputOptions =
  SputOptions
    <$> O.option O.auto (O.long "numOfBytes"
                      <> O.short 'b'
                      <> O.help "Number of bytes to be sent each times")
    <*> topicOption
    <*> O.option O.auto (O.long "pubLevel"
                      <> O.short 'l'
                      <> O.help "PubLevel, 0 or 1")

topicOption :: Parser TopicName
topicOption = a_topic <|> b_topic
  where
    a_topic = TopicName <$> O.strOption (O.long "topic" <> O.help "Topic Name")
    b_topic = O.flag' RandomTopicName (O.long "random-topic" <> O.help "Generate random topic name")

-------------------------------------------------------------------------------

sput :: Socket -> SputOptions -> IO ()
sput sock SputOptions{..} = do
  clientid <- preparePubRequest sock (fromIntegral pubLevel)
  showSpeed "sput" numOfBytes (action clientid pubLevel)
  where
    action clientid level = do
      topic <- case topicName of
                 TopicName name  -> return $ encodeUtf8 name
                 RandomTopicName -> genTopic
      HESP.sendMsg sock $ sputRequest clientid topic payload
      case level of
        0 -> return ()
        1 -> do _ <- HESP.recvMsgs sock 1024
                return ()
        _ -> error "Invalid pubLevel."
    payload = BS.replicate numOfBytes (fromIntegral $ ord 'x')

sputRequest :: ByteString -> ByteString -> ByteString -> HESP.Message
sputRequest clientid topic payload =
  let cs = [ HESP.mkBulkString "sput"
           , HESP.mkBulkString clientid
           , HESP.mkBulkString topic
           , HESP.mkBulkString payload
           ]
   in HESP.mkArrayFromList cs

preparePubRequest :: Socket -> Integer -> IO ByteString
preparePubRequest sock pubLevel =
  let mapping = [ (HESP.mkBulkString "pub-level", HESP.Integer pubLevel)
                , (HESP.mkBulkString "pub-method", HESP.Integer 1)
                ]
      hiCommand = HESP.mkArrayFromList [ HESP.mkBulkString "hi"
                                       , HESP.mkMapFromList mapping
                                       ]
   in do HESP.sendMsg sock hiCommand
         resps <- HESP.recvMsgs sock 1024
         case resps V.! 0 of
           Left x  -> error x
           Right m -> return $ extractClientId m

-------------------------------------------------------------------------------

extractClientId :: HESP.Message -> ByteString
extractClientId (HESP.MatchArray vs) =
  let t = fromJust $ HESP.getBulkStringParam vs 0
      r = fromJust $ HESP.getBulkStringParam vs 1
      i = fromJust $ HESP.getBulkStringParam vs 2
   in if t == "hi" && r == "OK" then i else error "clientid error"
extractClientId x = error $ "Unexpected message: " <> show x

showSpeed :: String -> Int -> IO a -> IO b
showSpeed label numBytes action = go 0 0.0
  where
    go :: Int -> Double -> IO b
    go flow time = do
      startTime <- getPOSIXTime
      _ <- action
      endTime <- getPOSIXTime
      let deltaT = realToFrac $ endTime - startTime
      let flow' = flow + numBytes
      let time' = time + deltaT
      if time' > 1
         then do let speed = (fromIntegral flow') / time' / 1024 / 1024
                 hPutStr SIO.stdout $ "\r=> " <> label <> " speed: "
                                   <> showFFloat (Just 2) speed " MiB/s"
                 go 0 0.0
         else go flow' time'

genTopic :: IO ByteString
genTopic = do
  x <- floor <$> getPOSIXTime :: IO Int
  return $ "topic-" <> (encodeUtf8 . show) x

encodeUtf8 :: String -> ByteString
encodeUtf8 = Text.encodeUtf8 . Text.pack
