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
import qualified Data.Text             as T
import           Data.Text.Encoding    (decodeUtf8)
import           Data.UUID             (toText)
import           Data.UUID.V4          (nextRandom)
import qualified Data.Vector           as V
import           Data.Word             (Word64)
import           Log.Store.Base        hiding (Env)
import qualified Network.HESP          as HESP
import qualified Network.HESP.Commands as HESP
import           Network.Socket        (Socket)
import qualified Network.Socket        as NS
import           Text.Read             (readMaybe)

import qualified Mangrove.Store        as Store
import           Mangrove.Types        (App, ClientId, ClientOption (..),
                                        Env (..), RequestType (..), getOption,
                                        insertOption)
import           Mangrove.Utils        ((.|.))
import qualified Mangrove.Utils        as U

-------------------------------------------------------------------------------

-- | Parse client request and then send response to client.
onRecvMsg :: Socket
          -> Context
          -> Either String HESP.Message
          -> App (Maybe ())
onRecvMsg sock _ (Left errmsg) = do
  Colog.logError $ "Failed to parse message: " <> T.pack errmsg
  HESP.sendMsg sock $ HESP.mkSimpleError "ERR" $ U.str2bs errmsg
  -- FIXME: should we close this connection?
  liftIO $ NS.gracefulClose sock (10 * 1000)  -- 10 seconds
  return Nothing
onRecvMsg sock ctx (Right msg) =
  case parseRequest msg of
    Left e    -> do
      Colog.logWarning $ decodeUtf8 e
      HESP.sendMsg sock $ HESP.mkSimpleError "ERR" e
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

parseHandshake :: V.Vector HESP.Message
               -> Either ByteString RequestType
parseHandshake paras = do
    dict  <- HESP.extractMapParam "Arguments" paras 0
    value <- HESP.extractMapField dict (HESP.mkBulkString "pubLevel")
    level <- getIntegerEither value "pubLevel"
    return $ Handshake level
  where
    getIntegerEither :: HESP.Message -> ByteString -> Either ByteString Integer
    getIntegerEither (HESP.Integer x) _ = Right x
    getIntegerEither _ label            = Left $ label <> " must be an integer."

parseSPut :: V.Vector HESP.Message
          -> Either ByteString RequestType
parseSPut paras = do
  cidStr  <- HESP.extractBulkStringParam "Client ID" paras 0
  cid     <- getClientIdEither cidStr
  topic   <- HESP.extractBulkStringParam "Topic"     paras 1
  payload <- HESP.extractBulkStringParam "Payload"   paras 2
  return   $ SPut cid topic payload

parseSPuts :: V.Vector HESP.Message
           -> Either ByteString RequestType
parseSPuts paras = do
  cidStr  <- HESP.extractBulkStringParam       "Client ID"  paras 0
  cid     <- getClientIdEither cidStr
  topic    <- HESP.extractBulkStringParam      "Topic"      paras 1
  payloads <- HESP.extractBulkStringArrayParam "Payload"    paras 2
  return $ SPuts cid topic payloads

parseSGet :: V.Vector HESP.Message
          -> Either ByteString RequestType
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
  uuid <- liftIO nextRandom
  Colog.logInfo $ "Generated ClientID: " <> toText uuid
  let option = ClientOption sock n
  liftIO $ insertOption uuid option serverStatus
  let resp = mkHandshakeResp uuid True
  Colog.logDebug $ T.pack ("Sending: " ++ show resp)
  HESP.sendMsg sock resp
  return $ Just ()
processHandshake _ _ _ = return Nothing

processSPut :: Socket -> Context -> RequestType -> App (Maybe ())
processSPut _ ctx (SPut cid topic payload) = do
  Env{..} <- ask
  option <- liftIO $ getOption cid serverStatus
  case option of
    Nothing               -> do
      Colog.logError $ "ClientID " <> T.pack (show cid) <> " is not found."
      return $ Just ()
    Just ClientOption{..} -> do
      Colog.logInfo $ "Writing " <> decodeUtf8 topic <> " ..."
      r <- liftIO $ Store.sput ctx topic payload
      case clientPubLevel of
        1 -> case r of
          Left e        -> do
            Colog.logException (e :: SomeException)
            let resp = mkSPutResp cid topic 0 False
            Colog.logDebug $ T.pack ("Sending: " ++ show resp)
            HESP.sendMsg clientSock resp
          Right entryID -> do
            let resp = mkSPutResp cid topic entryID True
            Colog.logDebug $ T.pack ("Sending: " ++ show resp)
            HESP.sendMsg clientSock resp
        _ -> return ()
      return $ Just ()
processSPut _ _ _ = return Nothing

processSPuts :: Socket -> Context -> RequestType -> App (Maybe ())
processSPuts _ ctx (SPuts cid topic payloads) = do
  Env{..} <- ask
  option <- liftIO $ getOption cid serverStatus
  case option of
    Nothing               -> do
      Colog.logError $ "ClientID " <> T.pack (show cid) <> " is not found."
      return $ Just ()
    Just ClientOption{..} -> do
      Colog.logInfo $ "Writing " <> decodeUtf8 topic <> " ..."
      r <- liftIO $ Store.sputs ctx topic payloads
      case clientPubLevel of
        1 -> case r of
          Left (entryIDs, e) -> do
            Colog.logException (e :: SomeException)
            let succIds = V.map (\x -> mkSPutResp cid topic x True) entryIDs
                errResp = mkSPutResp cid topic 0 False
            let resps = V.snoc succIds errResp
            Colog.logDebug $ T.pack ("Sending: " ++ show resps)
            HESP.sendMsgs clientSock resps
          Right entryIDs     -> do
            let resps = V.map (\x -> mkSPutResp cid topic x True) entryIDs
            Colog.logDebug $ T.pack ("Sending: " ++ show resps)
            HESP.sendMsgs clientSock resps
        _ -> return ()
      return $ Just ()
processSPuts _ _ _ = return Nothing

processSGet :: Socket -> Context -> RequestType -> App (Maybe ())
processSGet sock ctx (SGet topic sid eid maxn offset) = do
  Colog.logInfo $ "Reading " <> decodeUtf8 topic <> " ..."
  r <- liftIO $ Store.sget ctx topic sid eid offset maxn
  case r of
    Left e   -> do
      Colog.logException (e :: SomeException)
      let resp = mkSGetResp topic [] False
      Colog.logDebug $ "Sending: " <> (T.pack . show) resp
      HESP.sendMsg sock resp
    Right xs -> do
      let resp = mkSGetResp topic xs True
      Colog.logDebug $ "Sending: " <> (T.pack . show) resp
      HESP.sendMsg sock resp
  return $ Just ()
processSGet _ _ _ = return Nothing

-------------------------------------------------------------------------------
-- Messages send to client

mkHandshakeResp :: ClientId
                -> Bool
                -> HESP.Message
mkHandshakeResp uuid True =
  HESP.mkPushFromList "hi" [ HESP.mkBulkString . BSC.pack . show $ uuid ]
mkHandshakeResp _ False = undefined

mkSPutResp :: ClientId
           -> ByteString
           -> Word64
           -> Bool
           -> HESP.Message
mkSPutResp uuid topic entryID res =
  let status = if res then "OK" else "ERR"
      fin    = if res then U.encodeUtf8 entryID
                      else "Message storing failed."
   in HESP.mkPushFromList "sput" [ HESP.mkBulkString . BSC.pack . show $ uuid
                                 , HESP.mkBulkString topic
                                 , HESP.mkBulkString status
                                 , HESP.mkBulkString fin
                                 ]

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

-------------------------------------------------------------------------------
-- Helpers

validateInt :: ByteString -> ByteString -> Either ByteString (Maybe Word64)
validateInt label s
  | s == ""   = Right Nothing
  | otherwise = case readMaybe (BSC.unpack s) of
      Nothing -> Left $ label <> " must be an integer."
      Just x  -> Right (Just x)

getClientIdEither :: ByteString -> Either ByteString ClientId
getClientIdEither bs =
  case readMaybe (BSC.unpack bs) of
    Nothing   -> Left $ "Invalid UUID: " <> bs <> "."
    Just uuid -> Right uuid
