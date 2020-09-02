{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Mangrove.Server
  ( onRecvMsg
  ) where

import qualified Colog
import           Control.Exception        (SomeException, throw, try)
import           Control.Monad.Reader     (ask, liftIO)
import           Data.ByteString          (ByteString)
import qualified Data.ByteString.Char8    as BSC
import           Data.CaseInsensitive     (CI)
import qualified Data.CaseInsensitive     as CI
import           Data.Map.Strict          (Map)
import qualified Data.Sequence            as Seq
import qualified Data.Text                as Text
import           Data.Text.Encoding       (decodeUtf8)
import           Data.Vector              (Vector)
import qualified Data.Vector              as V
import           Data.Word                (Word64)
import           Network.Socket           (Socket)
import           Text.Read                (readMaybe)

import           Log.Store.Base           (Context, EntryID,
                                           LogStoreException (..))
import qualified Mangrove.Server.Response as I
import qualified Mangrove.Store           as Store
import           Mangrove.Types           (App, ClientId, Env (..))
import qualified Mangrove.Types           as T
import           Mangrove.Utils           ((.|.))
import qualified Mangrove.Utils           as U
import qualified Network.HESP             as HESP
import qualified Network.HESP.Commands    as HESP

-------------------------------------------------------------------------------

-- | Client request types
data RequestType
  = Handshake (Map HESP.Message HESP.Message)
  | SPut ClientId ByteString ByteString
  | SPuts ClientId ByteString (V.Vector ByteString)
  | SRange ClientId ByteString (Maybe Word64) (Maybe Word64) Integer Integer
  | XAdd ByteString ByteString
  | XRange ByteString (Maybe Word64) (Maybe Word64) (Maybe Integer)
  deriving (Show, Eq)

-- | Parse client request and then send response to client.
onRecvMsg :: Socket
          -> Context
          -> Either String HESP.Message
          -> App (Maybe ())
onRecvMsg sock _ (Left errmsg) = do
  Colog.logError $ "Failed to parse message: " <> Text.pack errmsg
  HESP.sendMsg sock $ I.mkGeneralError $ U.str2bs errmsg
  -- Outer function will catch all exceptions and do a clean job.
  errorWithoutStackTrace "Invalid TCP message!"
onRecvMsg sock ctx (Right msg) =
  case parseRequest msg of
    Left e    -> do
      Colog.logWarning $ decodeUtf8 e
      HESP.sendMsg sock $ I.mkGeneralError e
      return Nothing
    Right req -> processRequest sock ctx req

parseRequest :: HESP.Message
             -> Either ByteString RequestType
parseRequest msg = do
  (n, paras) <- HESP.commandParser msg
  case CI.mk n of
    ("hi"     :: CI ByteString) -> parseHandshake paras
    ("sput"   :: CI ByteString) -> parseSPuts paras <> parseSPut paras
    ("srange" :: CI ByteString) -> parseSRange paras
    ("xadd"   :: CI ByteString) -> parseXAdd paras
    ("xrange" :: CI ByteString) -> parseXRange paras
    _        -> Left $ "Unrecognized request " <> n <> "."

processRequest :: Socket -> Context -> RequestType -> App (Maybe ())
processRequest sock ctx rt = processHandshake sock ctx rt
                         .|. processSPut      sock ctx rt
                         .|. processSPuts     sock ctx rt
                         .|. processSRange    sock ctx rt
                         .|. processXAdd      sock ctx rt
                         .|. processXRange    sock ctx rt

-------------------------------------------------------------------------------
-- Parse client requests

parseHandshake :: Vector HESP.Message -> Either ByteString RequestType
parseHandshake paras = do
  options <- HESP.extractMapParam "Options" paras 0
  return $ Handshake options

parseSPut :: Vector HESP.Message -> Either ByteString RequestType
parseSPut paras = do
  cidStr  <- HESP.extractBulkStringParam "Client ID" paras 0
  cid     <- T.getClientIdFromASCIIBytes' cidStr
  topic   <- HESP.extractBulkStringParam "Topic"     paras 1
  payload <- HESP.extractBulkStringParam "Payload"   paras 2
  return $ SPut cid topic payload

parseSPuts :: Vector HESP.Message
           -> Either ByteString RequestType
parseSPuts paras = do
  cidStr   <- HESP.extractBulkStringParam      "Client ID" paras 0
  cid      <- T.getClientIdFromASCIIBytes' cidStr
  topic    <- HESP.extractBulkStringParam      "Topic"     paras 1
  payloads <- HESP.extractBulkStringArrayParam "Payload"   paras 2
  return $ SPuts cid topic payloads

parseSRange :: Vector HESP.Message -> Either ByteString RequestType
parseSRange paras = do
  cidStr <- HESP.extractBulkStringParam "Client ID" paras 0
  cid    <- T.getClientIdFromASCIIBytes' cidStr
  topic  <- HESP.extractBulkStringParam "Topic"     paras 1
  sids   <- HESP.extractBulkStringParam "Start ID"  paras 2
  sid    <- validateInt                 "Start ID"  sids ""
  eids   <- HESP.extractBulkStringParam "End ID"    paras 3
  eid    <- validateInt                 "End ID"    eids ""
  offset <- HESP.extractIntegerParam    "Offset"    paras 4
  maxn <- HESP.extractIntegerParam      "Maxn"      paras 5
  return $ SRange cid topic sid eid offset maxn

parseXAdd :: Vector HESP.Message -> Either ByteString RequestType
parseXAdd paras = do
  topic <- HESP.extractBulkStringParam "Topic" paras 0
  reqid <- HESP.extractBulkStringParam "Entry ID" paras 1
  _     <- validateBSSimple "*" reqid "Invalid stream ID specified as stream command argument"
  let kvs = V.drop 2 paras
  case V.length kvs > 0 && even (V.length kvs) of
    -- XXX: separate payload generating process
    True  -> return $ XAdd topic (HESP.serialize $ HESP.mkArray kvs)
    False -> Left "wrong number of arguments for 'xadd' command"

parseXRange :: Vector HESP.Message -> Either ByteString RequestType
parseXRange paras = do
  topic <- HESP.extractBulkStringParam "Topic"     paras 0
  sids  <- HESP.extractBulkStringParam "Start ID"  paras 1
  sid   <- validateIntSimple sids "-" "Invalid stream ID specified as stream command argument"
  eids  <- HESP.extractBulkStringParam "End ID"    paras 2
  eid   <- validateIntSimple eids "+" "Invalid stream ID specified as stream command argument"
  case V.length paras of
    3 -> return $ XRange topic sid eid Nothing
    5 -> do
      count  <- HESP.extractBulkStringParam "Option" paras 3
      _      <- validateBSSimple "count" count "syntax error"
      maxns  <- HESP.extractBulkStringParam "Maxn"   paras 4
      e_maxn <- validateIntSimple maxns "" "value is not an integer or out of range"
      case e_maxn of
        Just n -> return $ XRange topic sid eid (Just $ toInteger n)
        _      -> Left "value is not an integer or out of range"
    _ -> Left "syntax error"

-------------------------------------------------------------------------------
-- Process client requests

processHandshake :: Socket -> Context -> RequestType -> App (Maybe ())
processHandshake sock _ (Handshake opt) = do
  Env{..} <- ask
  cid <- liftIO T.newClientId
  client <- liftIO $ T.newClientStatus sock opt
  liftIO $ T.insertClient cid client serverStatus
  Colog.logInfo $ "Created client: " <> T.packClientId cid
  let resp = I.mkHandshakeRespSucc cid
  Colog.logDebug $ Text.pack ("Sending: " ++ show resp)
  HESP.sendMsg sock resp
  return $ Just ()
processHandshake _ _ _ = return Nothing

processSPut :: Socket -> Context -> RequestType -> App (Maybe ())
processSPut sock ctx (SPut cid topic payload) =
  let payloads = V.singleton payload
   in pub sock ctx "sput" cid topic payloads >> (return $ Just ())
processSPut _ _ _ = return Nothing

processSPuts :: Socket -> Context -> RequestType -> App (Maybe ())
processSPuts sock ctx (SPuts cid topic payloads) =
  pub sock ctx "sput" cid topic payloads >> (return $ Just ())
processSPuts _ _ _ = return Nothing

processSRange :: Socket -> Context -> RequestType -> App (Maybe ())
processSRange sock ctx (SRange cid topic sid eid offset maxn) = do
  let lcmd = "srange"
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
    Just client -> do
      let cut = Seq.take (fromInteger maxn) . Seq.drop (fromInteger offset)
      e_s <- liftIO . try $ cut <$> Store.readEntries ctx topic sid eid
      let clientSock = T.clientSocket client
      case e_s of
        Left e@(LogStoreLogNotFoundException _) -> do
          let errmsg = "Topic " <> topic <> " not found."
          Colog.logError . Text.pack . show $ e
          HESP.sendMsg clientSock $ I.mkGeneralPushError lcmd errmsg
        Left e -> throw e
        Right s -> do
          let msgs = I.mkElementResp cid lcmd topic <$> s
          HESP.sendMsgs clientSock msgs
          HESP.sendMsg clientSock $ I.mkCmdPush cid lcmd topic "DONE"
  return $ Just ()
processSRange _ _ _ = return Nothing

processXAdd :: Socket -> Context -> RequestType -> App (Maybe ())
processXAdd sock ctx (XAdd topic payload) = do
  r <- liftIO $ Store.sput ctx topic payload
  case r of
    Right entryID -> do
      liftIO $ HESP.sendMsg sock $ (HESP.mkBulkString . U.str2bs . show) entryID
    Left (e :: SomeException) -> do
      let errmsg = "Message storing failed: " <> (U.str2bs . show) e
      Colog.logWarning $ decodeUtf8 errmsg
      liftIO $ HESP.sendMsg sock $ I.mkGeneralError errmsg
  return $ Just ()
processXAdd _ _ _ = return Nothing

processXRange :: Socket -> Context -> RequestType -> App (Maybe ())
processXRange sock ctx (XRange topic sid eid maxn) = do
  let cut = case maxn of
        Nothing -> id
        Just n  -> Seq.take (fromInteger n)
  e_s <- liftIO . try $ cut <$> Store.readEntries ctx topic sid eid
  case e_s of
    Left (LogStoreLogNotFoundException _) -> do
      HESP.sendMsg sock $ HESP.mkArrayFromList []
    Left e  -> throw e
    Right s -> do
      let msgs = I.mkSimpleElementResp <$> s
      HESP.sendMsgs sock msgs
  return $ Just ()
processXRange _ _ _ = return Nothing

-------------------------------------------------------------------------------

pub :: Socket
    -> Context
    -> ByteString
    -> ClientId
    -> ByteString
    -> Vector ByteString
    -> App ()
pub sock ctx lcmd cid topic payloads = do
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
    Just client -> do
      Colog.logDebug $ "Writing " <> decodeUtf8 topic <> " ..."
      let e_level  = T.extractClientPubLevel  client
          e_method = T.extractClientPubMethod client
      let clientSock = T.clientSocket client
      case e_method of
        Right 0 -> do
          r <- liftIO $ Store.sputsAtom ctx topic payloads
          pubRespByLevel clientSock lcmd cid topic e_level r
        Right 1 -> do
          r <- liftIO $ Store.sputs ctx topic payloads
          pubRespByLevel clientSock lcmd cid topic e_level r
        Right x -> do
          let errmsg = "Unsupported pub-method: " <> (U.str2bs . show) x
          Colog.logWarning $ decodeUtf8 errmsg
          HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
        Left x -> do
          let errmsg = "Extract pub-method error: " <> (U.str2bs . show) x
          Colog.logWarning $ decodeUtf8 errmsg
          HESP.sendMsg clientSock $ I.mkGeneralPushError lcmd errmsg

pubRespByLevel :: Socket
               -> ByteString
               -> ClientId
               -> ByteString
               -> Either ByteString Integer
               -> Either (Vector EntryID, SomeException) (Vector EntryID)
               -> App ()
pubRespByLevel clientSock lcmd cid topic level r =
  case level of
    Right 0 -> return ()
    Right 1 -> case r of
      Left (entryIDs, e) -> do
        Colog.logException e
        let succIds = V.map (I.mkSPutRespSucc cid topic) entryIDs
            errResp = I.mkSPutRespFail cid topic "Message storing failed."
        let resps = V.snoc succIds errResp
        Colog.logDebug $ Text.pack ("Sending: " ++ show resps)
        HESP.sendMsgs clientSock resps
      Right entryIDs     -> do
        let resps = V.map (I.mkSPutRespSucc cid topic) entryIDs
        Colog.logDebug $ Text.pack ("Sending: " ++ show resps)
        HESP.sendMsgs clientSock resps
    Right x -> do
      let errmsg = "Unsupported pub-level: " <> (U.str2bs . show) x
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg clientSock $ I.mkGeneralPushError lcmd errmsg
    Left x  -> do
      let errmsg = "Extract pub-level error: " <> (U.str2bs . show) x
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg clientSock $ I.mkGeneralPushError lcmd errmsg

-------------------------------------------------------------------------------
-- Helpers

-- Return value of `validateInt` and `validateIntSimple`:
-- Left bs: validation failed
-- Right Nothing: it matched default value
-- Right (Just n): validation succeeded
validateInt :: ByteString -> ByteString -> ByteString -> Either ByteString (Maybe Word64)
validateInt label s defaultVal = validateIntSimple s defaultVal (label <> " must be an integer")

validateIntSimple :: ByteString -> ByteString -> ByteString -> Either ByteString (Maybe Word64)
validateIntSimple s defaultVal errMsg
  | CI.mk s == CI.mk defaultVal = Right Nothing
  | otherwise = case readMaybe (BSC.unpack s) of
      Nothing -> Left errMsg
      Just x  -> Right (Just x)

validateBSSimple :: ByteString -> ByteString -> ByteString -> Either ByteString ByteString
validateBSSimple expected real errMsg
  | CI.mk real == CI.mk expected = Right real
  | otherwise = Left errMsg
