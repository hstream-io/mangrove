{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Mangrove.Server
  ( onRecvMsg
  ) where

import qualified Colog
import           Control.Exception     (SomeException)
import           Control.Monad.Reader  (ask, liftIO)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.Text             as Text
import           Data.Text.Encoding    (decodeUtf8)
import           Data.Vector           (Vector)
import qualified Data.Vector           as V
import           Data.Word             (Word64)
import           Log.Store.Base        hiding (Env)
import qualified Network.HESP          as HESP
import qualified Network.HESP.Commands as HESP
import           Network.Socket        (Socket)
import qualified Network.Socket        as NS
import           Text.Read             (readMaybe)

import qualified Mangrove.Store        as Store
import           Mangrove.Types        (App, ClientId, ClientOptions (..),
                                        Env (..), RequestType (..))
import qualified Mangrove.Types        as T
import           Mangrove.Utils        ((.|.))
import qualified Mangrove.Utils        as U

-------------------------------------------------------------------------------

-- | Parse client request and then send response to client.
onRecvMsg :: Socket
          -> Context
          -> Either String HESP.Message
          -> App (Maybe ())
onRecvMsg sock _ (Left errmsg) = do
  Colog.logError $ "Failed to parse message: " <> Text.pack errmsg
  HESP.sendMsg sock $ mkGeneralError $ U.str2bs errmsg
  -- FIXME: should we close this connection?
  --
  -- Warning: the socket may be closed twice in a small time interval!
  -- This must be fixed in the future.
  liftIO $ NS.gracefulClose sock (10 * 1000)  -- 10 seconds
  return Nothing
onRecvMsg sock ctx (Right msg) =
  case parseRequest msg of
    Left e    -> do
      Colog.logWarning $ decodeUtf8 e
      HESP.sendMsg sock $ mkGeneralError e
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

-- TODO: delete client options by socket while sending error happens
processRequest :: Socket -> Context -> RequestType -> App (Maybe ())
processRequest sock ctx rt = processHandshake sock ctx rt
                         .|. processSPut      sock ctx rt
                         .|. processSPuts     sock ctx rt
                         .|. processSGet      sock ctx rt

-------------------------------------------------------------------------------
-- Parse client requests

parseHandshake :: Vector HESP.Message -> Either ByteString RequestType
parseHandshake paras = do
    dict  <- HESP.extractMapParam "Arguments" paras 0
    value <- HESP.extractMapField dict (HESP.mkBulkString "pubLevel")
    level <- getIntegerEither value "pubLevel"
    return $ Handshake level
  where
    getIntegerEither :: HESP.Message -> ByteString -> Either ByteString Integer
    getIntegerEither (HESP.Integer x) _ = Right x
    getIntegerEither _ label            = Left $ label <> " must be an integer."

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
  topic   <- HESP.extractBulkStringParam "Topic"              paras 0
  sids    <- HESP.extractBulkStringParam "Start ID"           paras 1
  sid     <- validateInt                 "Start ID"           sids
  eids    <- HESP.extractBulkStringParam "End ID"             paras 2
  eid     <- validateInt                 "End ID"             eids
  maxn    <- HESP.extractIntegerParam    "Max message number" paras 3
  offset  <- HESP.extractIntegerParam    "Offset"             paras 4
  return $ SGet topic sid eid maxn offset

-------------------------------------------------------------------------------
-- Process client requests

processHandshake :: Socket -> Context -> RequestType -> App (Maybe ())
processHandshake sock _ (Handshake n) = do
  Env{..} <- ask
  cid <- liftIO T.newClientId
  Colog.logDebug $ "Generated ClientID: " <> T.packClientId cid
  let options = ClientOptions { clientSock     = sock
                              , clientPubLevel = n
                              }
  liftIO $ T.insertClientOptions serverStatus cid options
  let resp = mkHandshakeRespSucc cid
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
processSGet sock ctx (SGet topic sid eid maxn offset) = do
  Colog.logInfo $ "Reading " <> decodeUtf8 topic <> " ..."
  r <- liftIO $ Store.sget ctx topic sid eid offset maxn
  case r of
    Left e   -> do
      Colog.logException (e :: SomeException)
      let resp = mkSGetResp topic [] False
      Colog.logDebug $ "Sending: " <> (Text.pack . show) resp
      HESP.sendMsg sock resp
    Right xs -> do
      let resp = mkSGetResp topic xs True
      Colog.logDebug $ "Sending: " <> (Text.pack . show) resp
      HESP.sendMsg sock resp
  return $ Just ()
processSGet _ _ _ = return Nothing

processPub :: Socket
           -> Context
           -> ByteString
           -> ClientId
           -> ByteString
           -> Vector ByteString
           -> App ()
processPub sock ctx lcmd cid topic payloads = do
  Env{..} <- ask
  option <- liftIO $ T.getClientOptions serverStatus cid
  case option of
    Nothing               -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg sock $ mkGeneralPushError lcmd errmsg
    Just ClientOptions{..} -> do
      Colog.logInfo $ "Writing " <> decodeUtf8 topic <> " ..."
      r <- liftIO $ Store.sputs ctx topic payloads
      case clientPubLevel of
        0 -> return ()
        1 -> case r of
          Left (entryIDs, e) -> do
            Colog.logException (e :: SomeException)
            let succIds = V.map (\x -> mkSPutResp cid topic x True) entryIDs
                errResp = mkSPutResp cid topic 0 False
            let resps = V.snoc succIds errResp
            Colog.logDebug $ Text.pack ("Sending: " ++ show resps)
            HESP.sendMsgs clientSock resps
          Right entryIDs     -> do
            let resps = V.map (\x -> mkSPutResp cid topic x True) entryIDs
            Colog.logDebug $ Text.pack ("Sending: " ++ show resps)
            HESP.sendMsgs clientSock resps
        _ -> do
          let errmsg = "Unsupported pubLevel: "
                    <> (U.str2bs . show) clientPubLevel
          Colog.logWarning $ decodeUtf8 errmsg
          HESP.sendMsg clientSock $ mkGeneralPushError lcmd errmsg

-------------------------------------------------------------------------------
-- Messages send to client

{-# INLINE mkHandshakeRespSucc #-}
mkHandshakeRespSucc :: ClientId
                    -> HESP.Message
mkHandshakeRespSucc cid =
  HESP.mkArrayFromList [ HESP.mkBulkString "hi"
                       , HESP.mkBulkString "OK"
                       , HESP.mkBulkString $ T.packClientIdBS cid
                       ]

{-# INLINE mkSPutResp #-}
mkSPutResp :: ClientId
           -> ByteString
           -> Word64
           -> Bool
           -> HESP.Message
mkSPutResp cid topic entryID res =
  let status = if res then "OK" else "ERR"
      fin    = if res then U.encodeUtf8 entryID
                      else "Message storing failed."
   in HESP.mkPushFromList "sput" [ HESP.mkBulkString $ T.packClientIdBS cid
                                 , HESP.mkBulkString topic
                                 , HESP.mkBulkString status
                                 , HESP.mkBulkString fin
                                 ]

{-# INLINE mkSGetResp #-}
mkSGetResp :: ByteString
           -> [(ByteString, Word64)]
           -> Bool
           -> HESP.Message
mkSGetResp topic contents res =
  let status = if res then "OK" else "ERR"
      fin    = if res then HESP.mkArrayFromList $ map cons contents
                      else HESP.mkBulkString "Message fetching failed"
      cons (p, i) = HESP.mkArrayFromList [ HESP.mkBulkString (U.encodeUtf8 i)
                                         , HESP.mkBulkString p
                                         ]
   in HESP.mkPushFromList "sget" [ HESP.mkBulkString topic
                                 , HESP.mkBulkString status
                                 , fin
                                 ]

{-# INLINE mkGeneralError #-}
mkGeneralError :: ByteString -> HESP.Message
mkGeneralError = HESP.mkSimpleError "ERR"

{-# INLINE mkGeneralPushError #-}
mkGeneralPushError :: ByteString -> ByteString -> HESP.Message
mkGeneralPushError pushtype errmsg =
  HESP.mkPushFromList pushtype [ HESP.mkBulkString "ERR"
                               , HESP.mkBulkString errmsg
                               ]

-------------------------------------------------------------------------------
-- Helpers

validateInt :: ByteString -> ByteString -> Either ByteString (Maybe Word64)
validateInt label s
  | s == ""   = Right Nothing
  | otherwise = case readMaybe (BSC.unpack s) of
      Nothing -> Left $ label <> " must be an integer."
      Just x  -> Right (Just x)
