{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Mangrove.Server
  ( onRecvMsg
  ) where

import qualified Colog
import           Control.Exception        (SomeException)
import           Control.Monad.Reader     (ask, liftIO)
import           Data.ByteString          (ByteString)
import qualified Data.ByteString.Char8    as BSC
import qualified Data.ByteString.Lazy     as LBS
import qualified Data.Text                as Text
import           Data.Text.Encoding       (decodeUtf8)
import           Data.Vector              (Vector)
import qualified Data.Vector              as V
import           Data.Word                (Word64)
import           Network.Socket           (Socket, close)
import           Text.Read                (readMaybe)

import           Log.Store.Base           (Context, EntryID)
import qualified Mangrove.Server.Response as I
import qualified Mangrove.Store           as Store
import qualified Mangrove.Streaming       as S
import           Mangrove.Types           (App, Env (..), RequestType (..))
import qualified Mangrove.Types           as T
import           Mangrove.Utils           ((.|.))
import qualified Mangrove.Utils           as U
import qualified Network.HESP             as HESP
import qualified Network.HESP.Commands    as HESP

-------------------------------------------------------------------------------

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
  case n of
    "hi"    -> parseHandshake paras
    "sput"  -> parseSPuts paras <> parseSPut paras
    "sget"  -> parseSGet paras
    "sgetc" -> parseSGetCtrl paras
    "bye"   -> parseClose paras
    _       -> Left $ "Unrecognized request " <> n <> "."

processRequest :: Socket -> Context -> RequestType -> App (Maybe ())
processRequest sock ctx rt = processHandshake sock ctx rt
                         .|. processSPut      sock ctx rt
                         .|. processSPuts     sock ctx rt
                         .|. processSGet      sock ctx rt
                         .|. processSGetCtrl  sock rt
                         .|. processClose     sock ctx rt

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

parseSGet :: Vector HESP.Message -> Either ByteString RequestType
parseSGet paras = do
  cidStr <- HESP.extractBulkStringParam "Client ID"          paras 0
  cid    <- T.getClientIdFromASCIIBytes' cidStr
  topic  <- HESP.extractBulkStringParam "Topic"              paras 1
  sids   <- HESP.extractBulkStringParam "Start ID"           paras 2
  sid    <- validateInt                 "Start ID"           sids
  eids   <- HESP.extractBulkStringParam "End ID"             paras 3
  eid    <- validateInt                 "End ID"             eids
  offset <- HESP.extractIntegerParam    "Offset"             paras 4
  return $ SGet cid topic sid eid offset

parseSGetCtrl :: Vector HESP.Message -> Either ByteString RequestType
parseSGetCtrl paras = do
  cidStr <- HESP.extractBulkStringParam "Client ID"          paras 0
  cid    <- T.getClientIdFromASCIIBytes' cidStr
  topic  <- HESP.extractBulkStringParam "Topic"              paras 1
  maxn   <- HESP.extractIntegerParam    "Max message number" paras 2
  return $ SGetCtrl cid topic maxn

parseClose :: Vector HESP.Message -> Either ByteString RequestType
parseClose paras = do
  cidStr <- HESP.extractBulkStringParam "Client ID"          paras 0
  cid    <- T.getClientIdFromASCIIBytes' cidStr
  return $ Close cid

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

processSGet :: Socket -> Context -> RequestType -> App (Maybe ())
processSGet sock ctx (SGet cid topic sid eid offset) = do
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError "sget" errmsg
    Just client -> do
      Colog.logDebug $ "Reading " <> decodeUtf8 topic <> " ..."
      let clientSock = T.clientSocket client
      -- NOTE: offset must not exceed maxBound::Int
      presget clientSock ctx "sget" cid topic sid eid (fromInteger offset)
  return $ Just ()
processSGet _ _ _ = return Nothing

processSGetCtrl :: Socket -> RequestType -> App (Maybe ())
processSGetCtrl sock (SGetCtrl cid topic maxn) =
  sgetctrl sock "sgetc" cid topic (fromInteger maxn) >> return (Just ())
processSGetCtrl _ _ = return Nothing

processClose :: Socket -> Context -> RequestType -> App (Maybe ())
processClose sock ctx (Close cid) = do
  Env{serverStatus = serverStatus} <- ask
  liftIO $ T.deleteClient cid serverStatus
  Colog.logInfo $ "Deleted client: " <> T.packClientId cid
  liftIO $ close sock
  return $ Just ()
processClose _ _ = return Nothing

-------------------------------------------------------------------------------

presget :: Socket
        -> Context
        -> ByteString
        -> T.ClientId
        -> ByteString
        -> Maybe EntryID
        -> Maybe EntryID
        -> Int
        -> App ()
presget sock ctx lcmd cid topic sid eid offset = do
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
    Just client -> do
      s <- S.drop offset <$> Store.readStreamEntry ctx topic sid eid
      is_succ <- liftIO $ T.tryInsertConsumeStream topic s client
      let resp = case is_succ of
                   False -> I.mkCmdPushError cid lcmd topic "already existed"
                   True  -> I.mkCmdPush cid lcmd topic "OK"
      HESP.sendMsg sock resp

sgetctrl :: Socket -> ByteString -> T.ClientId -> ByteString -> Int -> App ()
sgetctrl sock lcmd cid topic maxn = do
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logError $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
    Just client -> do
      mes <- liftIO $ T.takeConsumeElements client topic maxn
      case mes of
        Nothing -> do
          let errmsg = "elements not found"
          HESP.sendMsg sock $ I.mkCmdPushError cid lcmd topic errmsg
        Just es -> do
          let enc = HESP.serialize . I.mkElementResp cid lcmd topic
          -- FIXME: If SomeException happens, the client socket will be closed
          -- immediately. There is no error message send to clien.
          rbs <- liftIO $ S.encodeFromChunksIO enc es
          if LBS.null rbs
             then do liftIO $ T.deleteClientConsume topic client
                     HESP.sendMsg sock $ I.mkCmdPush cid lcmd topic "END"
             -- TODO: sub-level
             else do HESP.sendLazy sock rbs
                     HESP.sendMsg sock $ I.mkCmdPush cid lcmd topic "DONE"

pub :: Socket
    -> Context
    -> ByteString
    -> T.ClientId
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
               -> T.ClientId
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

validateInt :: ByteString -> ByteString -> Either ByteString (Maybe Word64)
validateInt label s
  | s == ""   = Right Nothing
  | otherwise = case readMaybe (BSC.unpack s) of
      Nothing -> Left $ label <> " must be an integer."
      Just x  -> Right (Just x)
