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
import qualified Data.Text                as Text
import           Data.Text.Encoding       (decodeUtf8)
import           Data.Vector              (Vector)
import qualified Data.Vector              as V
import           Data.Word                (Word64)
import           Network.Socket           (Socket)
import           Text.Read                (readMaybe)

import           Log.Store.Base           hiding (Env)
import qualified Mangrove.Server.Response as I
import qualified Mangrove.Store           as Store
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
    "hi"   -> parseHandshake paras
    "sput" -> parseSPuts paras <> parseSPut paras
    "sget" -> parseSGet paras
    _      -> Left $ "Unrecognized request " <> n <> "."

processRequest :: Socket -> Context -> RequestType -> App (Maybe ())
processRequest sock ctx rt = processHandshake sock ctx rt
                         .|. processSPut      sock ctx rt
                         .|. processSPuts     sock ctx rt
                         .|. processSGet      sock ctx rt

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
  maxn   <- HESP.extractIntegerParam    "Max message number" paras 4
  offset <- HESP.extractIntegerParam    "Offset"             paras 5
  return $ SGet cid topic sid eid maxn offset

-------------------------------------------------------------------------------
-- Process client requests

processHandshake :: Socket -> Context -> RequestType -> App (Maybe ())
processHandshake sock _ (Handshake opt) = do
  Env{..} <- ask
  cid <- liftIO T.newClientId
  let client = T.createClientStatus sock opt
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
   in processPub sock ctx "sput" cid topic payloads >> (return $ Just ())
processSPut _ _ _ = return Nothing

processSPuts :: Socket -> Context -> RequestType -> App (Maybe ())
processSPuts sock ctx (SPuts cid topic payloads) =
  processPub sock ctx "sput" cid topic payloads >> (return $ Just ())
processSPuts _ _ _ = return Nothing

processSGet :: Socket -> Context -> RequestType -> App (Maybe ())
processSGet sock ctx (SGet cid topic sid eid maxn offset) = do
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError "sget" errmsg
    Just _client -> do
      Colog.logDebug $ "Reading " <> decodeUtf8 topic <> " ..."
      r <- liftIO $ Store.sget ctx topic sid eid offset maxn
      case r of
        Left e   -> do
          Colog.logException (e :: SomeException)
          let resp = I.mkSGetRespFail cid topic
          Colog.logDebug $ "Sending: " <> (Text.pack . show) resp
          HESP.sendMsg sock resp
        Right xs -> do
          let resp = I.mkSGetRespSucc cid topic xs
          Colog.logDebug $ "Sending: " <> (Text.pack . show) resp
          HESP.sendMsg sock resp
  return $ Just ()
processSGet _ _ _ = return Nothing

processPub :: Socket
           -> Context
           -> ByteString
           -> T.ClientId
           -> ByteString
           -> Vector ByteString
           -> App ()
processPub sock ctx lcmd cid topic payloads = do
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
    Just client -> do
      Colog.logDebug $ "Writing " <> decodeUtf8 topic <> " ..."
      r <- liftIO $ Store.sputs ctx topic payloads
      let clientPubLevel = T.extractClientPubLevel client
      let clientSock = T.clientSocket client
      case clientPubLevel of
        Right 0 -> return ()
        Right 1 -> case r of
          Left (entryIDs, e) -> do
            Colog.logException (e :: SomeException)
            let succIds = V.map (\x -> I.mkSPutResp cid topic x True) entryIDs
                errResp = I.mkSPutResp cid topic 0 False
            let resps = V.snoc succIds errResp
            Colog.logDebug $ Text.pack ("Sending: " ++ show resps)
            HESP.sendMsgs clientSock resps
          Right entryIDs     -> do
            let resps = V.map (\x -> I.mkSPutResp cid topic x True) entryIDs
            Colog.logDebug $ Text.pack ("Sending: " ++ show resps)
            HESP.sendMsgs clientSock resps
        Right x -> do
          let errmsg = "Unsupported pubLevel: " <> (U.str2bs . show) x
          Colog.logWarning $ decodeUtf8 errmsg
          HESP.sendMsg clientSock $ I.mkGeneralPushError lcmd errmsg
        Left x -> do
          let errmsg = "Extract pubLevel error: " <> (U.str2bs . show) x
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
