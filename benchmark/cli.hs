{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main (main) where

import           Control.Applicative   ((<**>), (<|>))
import           Control.Concurrent    (MVar, forkIO, modifyMVar_, newMVar,
                                        swapMVar, threadDelay)
import           Control.Monad         (replicateM_)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as BS
import           Data.Char             (ord)
import qualified Data.List             as L
import           Data.Maybe            (fromJust)
import qualified Data.Text             as Text
import qualified Data.Text.Encoding    as Text
import           Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.UUID             as UUID
import qualified Data.UUID.V4          as UUID
import qualified Data.Vector           as V
import qualified Network.HESP          as HESP
import qualified Network.HESP.Commands as HESP
import           Network.Socket        (HostName, Socket)
import           Options.Applicative   (Parser)
import qualified Options.Applicative   as O
import           Text.Printf           (printf)

-------------------------------------------------------------------------------

data App =
  App { connection :: ConnectionSetting
      , command    :: Command
      }

appOpts :: Parser App
appOpts = App <$> connectionOption <*> commandOption

main :: IO ()
main = do
  App{..} <- O.execParser $ O.info (appOpts <**> O.helper) O.fullDesc
  case command of
    SputCommand opts -> runSputCommand connection opts
    SrangeCommand opts -> runSrangeCommand connection opts
runSputCommand :: ConnectionSetting -> SputOptions -> IO ()
runSputCommand conn opts = do
  clientLabel <- genRandomByteString
  var <- newMVar 0
  -- spawn clients
  replicateM_ (nClients opts) $ forkIO $ runTCPClient conn $ sput clientLabel opts var
  -- print speed
  let interval = floor $ (printInterval opts) * 1000000
  putStrLn $ printf "%-10s %-20s %-20s" ("Command" :: String) ("Speed (MiB/s)" :: String) ("AvgSpeed (MiB/s)" :: String)
  putStrLn $ replicate 48 '-'
  runPrintSpeed "sput" interval var

runSrangeCommand :: ConnectionSetting -> SrangeOptions -> IO ()
runSrangeCommand conn opts = do
  clientLabel <- genRandomByteString
  var <- newMVar 0
  -- spawn clients
  replicateM_ (rangeNClients opts) $ forkIO $ runTCPClient conn $ srange clientLabel opts var
  -- print speed
  let interval = floor $ (rangePrintInterval opts) * 1000000
  putStrLn $ printf "%-10s %-20s %-20s" ("Command" :: String) ("Speed (MiB/s)" :: String) ("AvgSpeed (MiB/s)" :: String)
  putStrLn $ replicate 48 '-'
  runPrintSpeed "srange" interval var

runTCPClient :: ConnectionSetting -> (Socket -> IO a) -> IO a
runTCPClient ConnectionSetting{..} f =
  HESP.connect serverHost (show serverPort) $ \(sock, _) -> f sock

-------------------------------------------------------------------------------

data ConnectionSetting =
  ConnectionSetting { serverHost :: HostName
                    , serverPort :: Int
                    }

connectionOption :: Parser ConnectionSetting
connectionOption =
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
                      <> O.value 6560
                      <> O.metavar "PORT"
                      <> O.help "Server port")

data Command = SputCommand SputOptions
             | SrangeCommand SrangeOptions

commandOption :: Parser Command
commandOption = SputCommand <$> O.hsubparser sputCommand
            <|> SrangeCommand <$> O.hsubparser srangeCommand
  where
    sputCommand = O.command "sput" (O.info sputOptions (O.progDesc "SPUT -- publish messages to stream"))
    srangeCommand = O.command "srange" (O.info srangeOptions (O.progDesc "SRANGE -- get messages from stream"))

data SputOptions =
  SputOptions { nClients         :: Int
              , numOfBytes       :: Int
              , topicName        :: TopicName
              , pubLevel         :: Int
              , pubMethod        :: Int
              , printInterval    :: Double
              , samplingInterval :: Double
              , maxTime          :: Double
              }

data TopicName = TopicName String
               | RandomTopicName
               | ClientTopicName String

data SrangeOptions =
  SrangeOptions { rangeNClients         :: Int
                , rangeTopicName        :: TopicName
                , rangeStartFrom        :: Integer
                , rangeMaxn             :: Integer
                , rangePrintInterval    :: Double
                , rangeSamplingInterval :: Double
                , rangeMaxTime          :: Double
                }

sputOptions :: Parser SputOptions
sputOptions =
  SputOptions
    <$> O.option O.auto (O.long "clients"
                      <> O.short 'n'
                      <> O.help "Number of clients")
    <*> O.option O.auto (O.long "bytes"
                      <> O.short 'b'
                      <> O.help "Number of bytes to be sent each times")
    <*> topicOption
    <*> O.option O.auto (O.long "pub-level"
                      <> O.short 'l'
                      <> O.showDefault
                      <> O.value 1
                      <> O.help "Pub-Level, 0 or 1")
    <*> O.option O.auto (O.long "pub-method"
                      <> O.short 'm'
                      <> O.showDefault
                      <> O.value 0
                      <> O.help "PubMethod, 0 or 1")
    <*> O.option O.auto (O.long "print-interval"
                      <> O.short 'i'
                      <> O.showDefault
                      <> O.value 1
                      <> O.help "Inteval of show speed to stdout, in seconds")
    <*> O.option O.auto (O.long "sampling-interval"
                      <> O.showDefault
                      <> O.value 0.1
                      <> O.help "Sampling interval, in seconds")
    <*> O.option O.auto (O.long "max-time"
                      <> O.short 'x'
                      <> O.help "Max seconds of running benchmark")

topicOption :: Parser TopicName
topicOption = a_topic <|> b_topic <|> c_topic
  where
    a_topic = TopicName <$> O.strOption (O.long "topic" <> O.help "Topic Name")
    b_topic = O.flag' RandomTopicName (O.long "random-topic" <> O.help "Generate random topic name")
    c_topic = ClientTopicName <$> O.strOption (O.long "client-topic" <> O.help "Topic Name with a unique random prefix for each client")

srangeOptions :: Parser SrangeOptions
srangeOptions =
  SrangeOptions
    <$> O.option O.auto (O.long "clients"
                      <> O.short 'n'
                      <> O.help "Number of clients")
    <*> topicOption
    <*> O.option O.auto (O.long "start"
                      <> O.short 's'
                      <> O.help "Message ID to start from")
    <*> O.option O.auto (O.long "maxn"
                      <> O.short 'm'
                      <> O.help "Number of messages to fetch each time")
    <*> O.option O.auto (O.long "print-interval"
                      <> O.short 'i'
                      <> O.showDefault
                      <> O.value 1
                      <> O.help "Inteval of show speed to stdout, in seconds")
    <*> O.option O.auto (O.long "sampling-interval"
                      <> O.showDefault
                      <> O.value 0.1
                      <> O.help "Sampling interval, in seconds")
    <*> O.option O.auto (O.long "max-time"
                      <> O.short 'x'
                      <> O.help "Max seconds of running benchmark")

-------------------------------------------------------------------------------

sput :: ByteString -> SputOptions -> MVar Int -> Socket -> IO ()
sput clientLabel SputOptions{..} sendedBytes sock = do
  clientid <- preparePubRequest sock (fromIntegral pubLevel) (fromIntegral pubMethod)
  speedSampling samplingInterval maxTime sendedBytes () (\_ -> action clientid pubLevel)
  where
    action clientid level = do
      topic <- case topicName of
                 TopicName name  -> return $ encodeUtf8 name
                 RandomTopicName -> genRandomByteString
                 ClientTopicName name -> return $ clientLabel <> encodeUtf8 name
      HESP.sendMsg sock $ sputRequest clientid topic payload
      case level of
        0 -> return ((), numOfBytes)
        1 -> do _ <- HESP.recvMsgs sock 1024
                -- TODO: assert result is OK
                return ((), numOfBytes)
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

preparePubRequest :: Socket -> Integer -> Integer -> IO ByteString
preparePubRequest sock pubLevel pubMethod =
  let mapping = [ (HESP.mkBulkString "pub-level", HESP.Integer pubLevel)
                , (HESP.mkBulkString "pub-method", HESP.Integer pubMethod)
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

srange :: ByteString -> SrangeOptions -> MVar Int -> Socket -> IO ()
srange clientLabel SrangeOptions{..} sendedBytes sock = do
  clientid <- prepareSrangeRequest sock
  speedSampling rangeSamplingInterval rangeMaxTime sendedBytes rangeStartFrom (action clientid) >> pure ()
  where
    action :: ByteString
           -> Integer
           -> IO (Integer, Int) -- (sid', flow)
    action clientid sid = do
      topic <- case rangeTopicName of
        TopicName name -> return $ encodeUtf8 name
        RandomTopicName -> error "srange does not support a single random topic."
        ClientTopicName name -> return $ clientLabel <> encodeUtf8 name
      HESP.sendMsg sock $ srangeRequest clientid topic
        (encodeUtf8 . show $ sid) "" 0 rangeMaxn
      datas <- HESP.recvMsgs sock 1024
      flows <- mapM processMsg datas
      let isEnd = V.last flows < 0
      let realDatas = if isEnd then V.init datas else datas
          realFlows = if isEnd then V.init flows else flows
          msgNum = L.length realDatas
      return (sid + toInteger msgNum, sum realFlows)
    processMsg :: Either String HESP.Message -> IO Int
    processMsg msg = do
      case msg of
        Right x ->
          case x of
            HESP.MatchPush "srange" args -> do
              let resp = fromJust $ HESP.getBulkStringParam args 2
              case resp of
                "OK"   -> do
                  -- let entryid = fromJust $ HESP.getBulkStringParam args 3
                  let entrydata = fromJust $ HESP.getBulkStringParam args 4
                      entrydataBytes = BS.length entrydata
                  -- print entryid
                  return entrydataBytes
                "DONE" -> return (-1)
                _      -> error "unexpected response"
            _                            -> error "unexpected command"
        Left _  -> error "unexpected message"

srangeRequest :: ByteString
              -> ByteString
              -> ByteString
              -> ByteString
              -> Integer
              -> Integer
              -> HESP.Message
srangeRequest clientid topic sid eid offset maxn =
  HESP.mkArrayFromList [ HESP.mkBulkString "srange"
                       , HESP.mkBulkString clientid
                       , HESP.mkBulkString topic
                       , HESP.mkBulkString sid
                       , HESP.mkBulkString eid
                       , HESP.Integer offset
                       , HESP.Integer maxn
                       ]

prepareSrangeRequest :: Socket -> IO ByteString
prepareSrangeRequest sock =
  let hiCommand = HESP.mkArrayFromList [ HESP.mkBulkString "hi"
                                       , HESP.mkMapFromList []
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

speedSampling :: forall a . Double -> Double -> MVar Int -> a -> (a -> IO (a, Int)) -> IO a
speedSampling interval maxTime numBytes start action = go 0 0.0 0.0 start
  where
    go :: Int -> Double -> Double -> a -> IO a
    go flow time totalTime input = do
      if totalTime > maxTime
         then swapMVar numBytes (-1) >> return input
         else do
           startTime <- getPOSIXTime
           (output, bytes) <- action input
           endTime <- getPOSIXTime
           let deltaT = realToFrac $ endTime - startTime
               flow'  = flow + bytes
               time'  = time + deltaT
               total' = totalTime + deltaT
           if time' > interval
              then do modifyMVar_ numBytes $ \s -> return (s + flow')
                      go 0 0.0 total' output
              else go flow' time' total' output

runPrintSpeed :: String -> Int -> MVar Int -> IO ()
runPrintSpeed label microsec numBytes = go 0 0
  where
    go totalBytes totalTime = do
      time <- getPOSIXTime
      threadDelay microsec
      bytes <- swapMVar numBytes 0
      if bytes < 0
         then return ()
         else do time' <- getPOSIXTime
                 let deltaT = realToFrac $ time' - time
                     totalBytes' = totalBytes + bytes
                     totalTime' = totalTime + deltaT
                 let speed = (fromIntegral bytes) / deltaT / 1024 / 1024
                     avgSpeed = (fromIntegral totalBytes') / totalTime' / 1024 / 1024
                 putStrLn $ printf "%-10s %-20.2f %-20.2f" label (speed :: Double) (avgSpeed :: Double)
                 go (totalBytes + bytes) (totalTime + deltaT)

genRandomByteString :: IO ByteString
genRandomByteString = UUID.toASCIIBytes <$> UUID.nextRandom

encodeUtf8 :: String -> ByteString
encodeUtf8 = Text.encodeUtf8 . Text.pack
