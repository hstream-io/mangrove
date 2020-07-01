{-# LANGUAGE OverloadedStrings #-}

-- | Messages send to client.
module Mangrove.Server.Response
  ( mkHandshakeRespSucc
  , mkSPutResp
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

{-# INLINE mkSPutResp #-}
mkSPutResp :: T.ClientId
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

{-# INLINE mkSGetRespSucc #-}
mkSGetRespSucc :: T.ClientId
               -> ByteString
               -> [(ByteString, Word64)]
               -> HESP.Message
mkSGetRespSucc cid topic contents =
  let pair (p, i) = HESP.mkArrayFromList [ HESP.mkBulkString (U.encodeUtf8 i)
                                         , HESP.mkBulkString p
                                         ]
   in HESP.mkPushFromList "sget" [ HESP.mkBulkString $ T.packClientIdBS cid
                                 , HESP.mkBulkString topic
                                 , HESP.mkBulkString "OK"
                                 , HESP.mkArrayFromList $ map pair contents
                                 ]

{-# INLINE mkSGetRespDone #-}
mkSGetRespDone :: T.ClientId -> ByteString -> HESP.Message
mkSGetRespDone cid topic =
  HESP.mkPushFromList "sget" [ HESP.mkBulkString $ T.packClientIdBS cid
                             , HESP.mkBulkString "DONE"
                             , HESP.mkBulkString topic
                             ]

{-# INLINE mkSGetRespFail #-}
mkSGetRespFail :: T.ClientId -> ByteString -> HESP.Message
mkSGetRespFail cid topic =
  HESP.mkPushFromList "sget" [ HESP.mkBulkString $ T.packClientIdBS cid
                             , HESP.mkBulkString "ERR"
                             , HESP.mkBulkString topic
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
