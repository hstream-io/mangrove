{-# LANGUAGE OverloadedStrings #-}

-- | Messages send to client.
module Mangrove.Server.Response
  ( mkHandshakeRespSucc
  , mkSPutRespSucc
  , mkSPutRespFail
  , mkSGetRespSucc
  , mkSGetRespDone
  , mkSGetRespFail
  , mkGeneralError
  , mkGeneralPushError
  ) where

import           Data.ByteString (ByteString)
import           Data.Word       (Word64)

import qualified Mangrove.Types  as T
import qualified Mangrove.Utils  as U
import qualified Network.HESP    as HESP

-------------------------------------------------------------------------------

{-# INLINE mkHandshakeRespSucc #-}
mkHandshakeRespSucc :: T.ClientId
                    -> HESP.Message
mkHandshakeRespSucc cid =
  HESP.mkArrayFromList [ HESP.mkBulkString "hi"
                       , HESP.mkBulkString "OK"
                       , HESP.mkBulkString $ T.packClientIdBS cid
                       ]

{-# INLINE mkSPutRespSucc #-}
mkSPutRespSucc :: T.ClientId
               -> ByteString
               -> Word64
               -> HESP.Message
mkSPutRespSucc cid topic entryID =
  HESP.mkPushFromList "sput" [ HESP.mkBulkString $ T.packClientIdBS cid
                             , HESP.mkBulkString topic
                             , HESP.mkBulkString "OK"
                             , HESP.mkBulkString $ U.encodeUtf8 entryID
                             ]

{-# INLINE mkSPutRespFail #-}
mkSPutRespFail :: T.ClientId
               -> ByteString
               -> ByteString
               -> HESP.Message
mkSPutRespFail cid topic errmsg =
  HESP.mkPushFromList "sput" [ HESP.mkBulkString $ T.packClientIdBS cid
                             , HESP.mkBulkString topic
                             , HESP.mkBulkString "ERR"
                             , HESP.mkBulkString errmsg
                             ]

{-# INLINE mkSGetRespSucc #-}
mkSGetRespSucc :: T.ClientId
               -> ByteString
               -> (ByteString, Word64)
               -> HESP.Message
mkSGetRespSucc cid topic (p, i) =
  HESP.mkPushFromList "sget" [ HESP.mkBulkString $ T.packClientIdBS cid
                             , HESP.mkBulkString topic
                             , HESP.mkBulkString "OK"
                             , HESP.mkBulkString (U.encodeUtf8 i)
                             , HESP.mkBulkString p
                             ]

{-# INLINE mkSGetRespDone #-}
mkSGetRespDone :: T.ClientId -> ByteString -> HESP.Message
mkSGetRespDone cid topic =
  HESP.mkPushFromList "sget" [ HESP.mkBulkString $ T.packClientIdBS cid
                             , HESP.mkBulkString topic
                             , HESP.mkBulkString "DONE"
                             ]

{-# INLINE mkSGetRespFail #-}
mkSGetRespFail :: T.ClientId -> ByteString -> HESP.Message
mkSGetRespFail cid topic =
  HESP.mkPushFromList "sget" [ HESP.mkBulkString $ T.packClientIdBS cid
                             , HESP.mkBulkString topic
                             , HESP.mkBulkString "ERR"
                             , HESP.mkBulkString "Message fetching failed"
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
