{-# LANGUAGE OverloadedStrings #-}

module Mangrove.Server
  ( onRecvMsg
  ) where

import qualified Colog
import           Control.Exception     (SomeException)
import           Control.Monad.Reader  (liftIO)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.Text             as T
import           Data.Text.Encoding    (decodeUtf8)
import qualified Data.Vector           as V
import           Data.Word             (Word64)
import           Log.Store.Base        hiding (Env)
import qualified Network.HESP          as HESP
import qualified Network.HESP.Commands as HESP
import           Network.Socket        (Socket)
import qualified Network.Socket        as NS
import           Text.Read             (readMaybe)

import qualified Mangrove.Store        as Store
import           Mangrove.Types        (App, RequestType (..))
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
    "sput" -> parseSPuts paras <> parseSPut paras
    "sget" -> parseSGet paras
    _      -> Left $ "Unrecognized request " <> n <> "."

processRequest :: Socket -> Context -> RequestType -> App (Maybe ())
processRequest sock ctx rt = processSPut  sock ctx rt
                         .|. processSPuts sock ctx rt
                         .|. processSGet  sock ctx rt

-------------------------------------------------------------------------------
-- Parse client requests

parseSPut :: V.Vector HESP.Message
          -> Either ByteString RequestType
parseSPut paras = do
  topic   <- HESP.extractBulkStringParam "Topic"   paras 0
  payload <- HESP.extractBulkStringParam "Payload" paras 1
  return   $ SPut topic payload

parseSPuts :: V.Vector HESP.Message
           -> Either ByteString RequestType
parseSPuts paras = do
  topic    <- HESP.extractBulkStringParam      "Topic"   paras 0
  payloads <- HESP.extractBulkStringArrayParam "Payload" paras 1
  return $ SPuts topic payloads

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

processSPut :: Socket -> Context -> RequestType -> App (Maybe ())
processSPut sock ctx (SPut topic payload) = do
  Colog.logInfo $ "Writing " <> decodeUtf8 topic <> " ..."
  r <- liftIO $ Store.sput ctx topic payload
  case r of
    Left e        -> do
      Colog.logException (e :: SomeException)
      let resp = mkSPutResp topic 0 False
      Colog.logDebug $ T.pack ("Sending: " ++ show resp)
      HESP.sendMsg sock resp
    Right entryID -> do
      let resp = mkSPutResp topic entryID True
      Colog.logDebug $ T.pack ("Sending: " ++ show resp)
      HESP.sendMsg sock resp
  return $ Just ()
processSPut _ _ _ = return Nothing

processSPuts :: Socket -> Context -> RequestType -> App (Maybe ())
processSPuts sock ctx (SPuts topic payloads) = do
  Colog.logInfo $ "Writing " <> decodeUtf8 topic <> " ..."
  r <- liftIO $ Store.sputs ctx topic payloads
  case r of
    Left (entryIDs, e) -> do
      Colog.logException (e :: SomeException)
      let succIds = V.map (\x -> mkSPutResp topic x True) entryIDs
          errResp = mkSPutResp topic 0 False
      let resps = V.snoc succIds errResp
      Colog.logDebug $ T.pack ("Sending: " ++ show resps)
      HESP.sendMsgs sock resps
    Right entryIDs     -> do
      let resps = V.map (\x -> mkSPutResp topic x True) entryIDs
      Colog.logDebug $ T.pack ("Sending: " ++ show resps)
      HESP.sendMsgs sock resps
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

mkSPutResp :: ByteString
           -> Word64
           -> Bool
           -> HESP.Message
mkSPutResp topic entryID res =
  let status = if res then "OK" else "ERR"
      fin    = if res then U.encodeUtf8 entryID
                      else "Message storing failed."
   in HESP.mkPushFromList "sput" [ HESP.mkBulkString topic
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
