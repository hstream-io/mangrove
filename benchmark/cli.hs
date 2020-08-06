{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main (main) where

import           Control.Applicative   ((<**>), (<|>))
import           Control.Concurrent    (Chan, MVar)
import qualified Control.Concurrent    as Conc
import           Control.Monad         (replicateM, void, when)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as BS
import qualified Data.ByteString.Char8 as BC
import           Data.Char             (ord)
import           Data.Maybe            (fromJust)
import qualified Data.Text             as Text
import qualified Data.Text.Encoding    as Text
import           Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.UUID             as UUID
import qualified Data.UUID.V4          as UUID
import           Data.Vector           (Vector)
import qualified Data.Vector           as V
import qualified Network.HESP          as HESP
import qualified Network.HESP.Commands as HESP
import           Network.Socket        (HostName, Socket)
import           Options.Applicative   (Parser)
import qualified Options.Applicative   as O
import           Text.Printf           (printf)

-------------------------------------------------------------------------------

data App =
  App { options :: AppOptions
      , command :: Command
      }

app :: Parser App
app = App <$> appOptions <*> commandOptions

main :: IO ()
main = do
  App{..} <- O.execParser $ O.info (app <**> O.helper) O.fullDesc
  case command of
    SputCommand cmdOpts   -> runSputCommand options cmdOpts
    SrangeCommand cmdOpts -> runSrangeCommand options cmdOpts

runSputCommand :: AppOptions -> SputOptions -> IO ()
runSputCommand AppOptions{..} opts = do
  sendedBytesVar <- Conc.newMVar 0
  clientLabelsChan <- Conc.newChan
  Conc.writeList2Chan clientLabelsChan [1..nClients]
  let action = sput opts samplingInterval maxTime clientLabelsChan sendedBytesVar
  -- spawn clients
  waitAllClients verbose =<< spawnClients nClients serverHost serverPort action
  runPrintSpeed "sput" (floor $ printInterval * 1000000) maxTime sendedBytesVar

runSrangeCommand :: AppOptions -> SrangeOptions -> IO ()
runSrangeCommand AppOptions{..} opts = do
  receivedBytes <- Conc.newMVar 0
  clientLabelsChan <- Conc.newChan
  Conc.writeList2Chan clientLabelsChan [1..nClients]
  let action = srange verbose opts samplingInterval maxTime clientLabelsChan receivedBytes (rangeStartFrom opts)
  -- spawn clients
  waitAllClients verbose =<< spawnClients nClients serverHost serverPort action
  runPrintSpeed "srange" (floor $ printInterval * 1000000) maxTime receivedBytes

-------------------------------------------------------------------------------

data AppOptions =
  AppOptions { serverHost       :: HostName
             , serverPort       :: Int
             , nClients         :: Int
             , printInterval    :: Double
             , samplingInterval :: Double
             , maxTime          :: Double
             , verbose          :: Bool
             }

appOptions :: Parser AppOptions
appOptions =
  AppOptions
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
    <*> O.option O.auto (O.long "clients"
                      <> O.short 'c'
                      <> O.help "Number of clients")
    <*> O.option O.auto (O.long "print-interval"
                      <> O.short 'i'
                      <> O.showDefault
                      <> O.value 1
                      <> O.help "Inteval of show speed to stdout, in seconds")
    <*> O.option O.auto (O.long "sampling-interval"
                      <> O.short 's'
                      <> O.showDefault
                      <> O.value 0.1
                      <> O.help "Sampling interval, in seconds")
    <*> O.option O.auto (O.long "print-max-time"
                      <> O.short 't'
                      <> O.help "Max time of show speed to stdout, in seconds")
    <*> O.flag False True (O.long "verbose"
                        <> O.short 'v'
                        <> O.help "Verbose")

-------------------------------------------------------------------------------

data Command = SputCommand SputOptions
             | SrangeCommand SrangeOptions

commandOptions :: Parser Command
commandOptions = SputCommand <$> O.hsubparser sputCommand
             <|> SrangeCommand <$> O.hsubparser srangeCommand
  where
    sputCommand = O.command "sput" (O.info sputOptions (O.progDesc "SPUT -- publish messages to stream"))
    srangeCommand = O.command "srange" (O.info srangeOptions (O.progDesc "SRANGE -- get messages from stream"))

data TopicName = TopicName String
               | ClientTopicName String

topicOption :: Parser TopicName
topicOption = a_topic <|> b_topic
  where
    a_topic = TopicName <$> O.strOption (O.long "topic" <> O.help "Topic Name")
    b_topic = ClientTopicName <$> O.strOption (O.long "client-topic" <> O.help "Topic Name with an index prefix for each client")

data ProducerTopicName = PTopicName TopicName
                       | PRandomTopicName

producerTopicOption :: Parser ProducerTopicName
producerTopicOption = a_topic <|> b_topic
  where
    a_topic = PTopicName <$> topicOption
    b_topic = O.flag' PRandomTopicName (O.long "random-topic" <> O.help "Generate random topic name")

data ConsumerTopicName = CTopicName TopicName

consumerTopicOption :: Parser ConsumerTopicName
consumerTopicOption = CTopicName <$> topicOption

-------------------------------------------------------------------------------
-- Write

data SputOptions =
  SputOptions { writeTopicName  :: ProducerTopicName
              , writeNumOfBytes :: Int
              , writeBatchSize  :: Int
              , pubLevel        :: Int
              , pubMethod       :: Int
              }

sputOptions :: Parser SputOptions
sputOptions =
  SputOptions
    <$> producerTopicOption
    <*> O.option O.auto (O.long "bytes"
                      <> O.short 'b'
                      <> O.help "Number of bytes to be sent each times")
    <*> O.option O.auto (O.long "batch"
                      <> O.short 'n'
                      <> O.help "Size of batch sending")
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

sput :: SputOptions
     -> Double -> Double
     -> Chan Int -> MVar Int
     -> Socket
     -> IO ()
sput SputOptions{..} samplingInterval maxTime clientLabels sendedBytes sock = do
  clientid <- preparePubRequest sock (fromIntegral pubLevel) (fromIntegral pubMethod)
  clientLabel <- BC.pack . show <$> Conc.readChan clientLabels
  speedSampling samplingInterval maxTime sendedBytes (action clientid clientLabel pubLevel)
  where
    action clientid label level = do
      topic <- genTopic label writeTopicName
      HESP.sendMsg sock $ sputRequest clientid topic payloads
      case level of
        0 -> return $ writeNumOfBytes * writeBatchSize
        1 -> do _ <- HESP.recvMsgs sock 1024
                -- TODO: assert result is OK
                return $ writeNumOfBytes * writeBatchSize
        _ -> error "Invalid pubLevel."
    genTopic label = \case
      PTopicName (TopicName name)       -> return $ encodeUtf8 name
      PTopicName (ClientTopicName name) -> return $ label <> encodeUtf8 name
      PRandomTopicName                  -> genRandomByteString
    payloads = V.replicate writeBatchSize $
      HESP.mkBulkString $ BS.replicate writeNumOfBytes (fromIntegral $ ord 'x')

sputRequest :: ByteString -> ByteString -> Vector HESP.Message -> HESP.Message
sputRequest clientid topic payloads =
  let cs = [ HESP.mkBulkString "sput"
           , HESP.mkBulkString clientid
           , HESP.mkBulkString topic
           , HESP.mkArray payloads
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
-- Read

data SrangeOptions =
  SrangeOptions { readTopicName  :: ConsumerTopicName
                , rangeStartFrom :: Integer
                , rangeMaxn      :: Integer
                , rangeIsEager   :: Bool
                }

srangeOptions :: Parser SrangeOptions
srangeOptions =
  SrangeOptions
    <$> consumerTopicOption
    <*> O.option O.auto (O.long "start"
                      <> O.short 's'
                      <> O.help "Message ID to start from")
    <*> O.option O.auto (O.long "maxn"
                      <> O.short 'n'
                      <> O.help "Max Number of messages to fetch each time")
    <*> O.flag False True (O.long "eager"
                        <> O.help "Should we continue consuming if reach the end (at this moment)")

srange :: Bool
       -> SrangeOptions
       -> Double -> Double
       -> Chan Int -> MVar Int -> Integer
       -> Socket
       -> IO ()
srange verbose SrangeOptions{..} samplingInterval maxTime clientLabels receivedBytes startOffset sock = do
  clientid <- prepareSrangeRequest sock
  lastState <- Conc.newMVar (startOffset, True)
  clientLabel <- BC.pack . show <$> Conc.readChan clientLabels
  speedSampling samplingInterval maxTime receivedBytes
                (action clientid clientLabel lastState)
  where
    action :: ByteString -> ByteString -> MVar (Integer, Bool) -> IO Int
    action clientid label lastState = do
      topic <- genTopic label readTopicName
      (sid, isDone) <- Conc.takeMVar lastState
      when isDone $
        HESP.sendMsg sock $
          srangeRequest clientid topic (encodeUtf8 . show $ sid) "" 0 rangeMaxn
      datas <- HESP.recvMsgs sock 1024
      (lastOffset, bytes, currentDone) <- processSRangeResps verbose datas
      if lastOffset < 0
         then if rangeIsEager
                 then do Conc.putMVar lastState (sid, currentDone)
                         return bytes
                 else return (-1)
         else do Conc.putMVar lastState (lastOffset + 1, currentDone)
                 return bytes
    genTopic label = \case
      CTopicName (TopicName name)       -> return $ encodeUtf8 name
      CTopicName (ClientTopicName name) -> return $ label <> encodeUtf8 name

processSRangeResps :: Bool
                   -> Vector (Either String HESP.Message)
                   -> IO (Integer, Int, Bool)
processSRangeResps verbose msgs = do
  rs <- V.mapM processMsg msgs
  let isDone = (fst . V.last) rs < 0
      len = V.length rs
  let lastOffset = fst $ rs V.! (if len > 1 then len - 2 else 0)
      totalBytes = V.sum . V.map snd $ (if isDone then V.init else id) rs
  return (lastOffset, totalBytes, isDone)
  where
    processMsg :: Either String HESP.Message -> IO (Integer, Int)
    processMsg (Right x) =
      case x of
        HESP.MatchPush "srange" args -> do
          let resp = fromJust $ HESP.getBulkStringParam args 2
          case resp of
            "OK"   -> do
              let entryid = fst . fromJust . BC.readInteger . fromJust $ HESP.getBulkStringParam args 3
                  entrylen = BS.length $ fromJust $ HESP.getBulkStringParam args 4
              return (entryid, entrylen)
            "DONE" -> do
              when verbose $ do
                let topic = fromJust $ HESP.getBulkStringParam args 1
                putStrLn $ "-> consume topic " <> decodeUtf8 topic <> " done."
              return (-1, -1)
            _      -> error "unexpected response"
        _ -> error "unexpected command"
    processMsg (Left _) = error "unexpected message"

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

spawnClients :: Int -> HostName -> Int -> (Socket -> IO ()) -> IO [MVar ()]
spawnClients n host port action = replicateM n $ do
  connHdl <- Conc.newEmptyMVar
  _ <- Conc.forkIO $
    HESP.connect host (show port) $ \(sock, _) -> Conc.putMVar connHdl () >> action sock
  return connHdl

waitAllClients :: Bool -> [MVar ()] -> IO ()
waitAllClients verbose connHdls = do
  when verbose $ putStrLn "-> Waiting for all of connections established..."
  mapM_ Conc.takeMVar connHdls
  when verbose $ putStrLn "-> Done"

endBytesFlag :: Int
endBytesFlag = -1

speedSampling :: Double -> Double -> MVar Int -> IO Int -> IO ()
speedSampling interval maxTime numBytes action = go 0 0.0 0.0
  where
    go flow time totalTime = do
      if totalTime > maxTime
         then void $ Conc.swapMVar numBytes endBytesFlag
         else do
           startTime <- getPOSIXTime
           bytes <- action
           endTime <- getPOSIXTime
           if bytes >= 0
              then do
                let deltaT = realToFrac $ endTime - startTime
                    flow'  = flow + bytes
                    time'  = time + deltaT
                    total' = totalTime + deltaT
                if time' > interval
                   then do Conc.modifyMVar_ numBytes $ \s ->
                             if s >= 0 then return (s + flow') else return s
                           go 0 0.0 total'
                   else go flow' time' total'
              else void $ Conc.swapMVar numBytes endBytesFlag

runPrintSpeed :: String -> Int -> Double -> MVar Int -> IO ()
runPrintSpeed label microsec maxTime numBytes = do
  putStrLn $ printf "%-10s %-20s %-20s" ("Command" :: String) ("Speed (MiB/s)" :: String) ("AvgSpeed (MiB/s)" :: String)
  putStrLn $ replicate 48 '-'
  go 0 0
  where
    go totalBytes totalTime = do
      time <- getPOSIXTime
      Conc.threadDelay microsec
      bytes <- Conc.swapMVar numBytes 0
      -- negtive bytes mean this is the end
      if bytes < 0 || maxTime < totalTime
         then return ()
         else do time' <- getPOSIXTime
                 let deltaT = realToFrac $ time' - time
                     totalBytes' = totalBytes + bytes
                     totalTime' = totalTime + deltaT
                 let speed = (fromIntegral bytes) / deltaT / 1024 / 1024
                     avgSpeed = (fromIntegral totalBytes') / totalTime' / 1024 / 1024
                 putStrLn $ printf "%-10s %-20.2f %-20.2f" label (speed :: Double) (avgSpeed :: Double)
                 go (totalBytes + bytes) (totalTime + deltaT)

extractClientId :: HESP.Message -> ByteString
extractClientId (HESP.MatchArray vs) =
  let t = fromJust $ HESP.getBulkStringParam vs 0
      r = fromJust $ HESP.getBulkStringParam vs 1
      i = fromJust $ HESP.getBulkStringParam vs 2
   in if t == "hi" && r == "OK" then i else error "clientid error"
extractClientId x = error $ "Unexpected message: " <> show x

genRandomByteString :: IO ByteString
genRandomByteString = UUID.toASCIIBytes <$> UUID.nextRandom

encodeUtf8 :: String -> ByteString
encodeUtf8 = Text.encodeUtf8 . Text.pack

decodeUtf8 :: ByteString -> String
decodeUtf8 = Text.unpack . Text.decodeUtf8
