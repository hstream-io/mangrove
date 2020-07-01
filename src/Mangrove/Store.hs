{-# LANGUAGE OverloadedStrings #-}

module Mangrove.Store
  ( sget
  , sput
  , sputs
  ) where

import           Control.Exception      (Exception, try)
import           Control.Monad.IO.Class (MonadIO)
import           Control.Monad.Reader   (runReaderT)
import           Data.ByteString        (ByteString)
import           Data.Either            (fromLeft, fromRight, isLeft)
import           Data.Vector            (Vector)
import qualified Data.Vector            as V
import           Streamly               (Serial, serially)
import qualified Streamly.Prelude       as S

import qualified Log.Store.Base         as LogStore
import           Mangrove.Utils         (bs2str)

-------------------------------------------------------------------------------

-- | Get a "snapshot" of elements from a stream.
sget :: Exception e
     => LogStore.Context          -- ^ db context
     -> ByteString                -- ^ topic
     -> Maybe LogStore.EntryID    -- ^ start entry id
     -> Maybe LogStore.EntryID    -- ^ end entry id
     -> Integer                   -- ^ offset
     -> Integer                   -- ^ max size
     -> IO (Either e [Element])
sget db topic start end offset maxn = try $ do
  xs <- readEntries db topic start end
  return $ take (fromInteger maxn) . drop (fromInteger offset) $ xs

-- | Put an element to a stream.
sput :: Exception e
     => LogStore.Context    -- ^ db context
     -> ByteString          -- ^ topic
     -> ByteString          -- ^ payload
     -> IO (Either e LogStore.EntryID)
sput db topic payload = try $ appendEntry db topic payload

-- | Put elements to a stream.
--
-- If SomeException happens, the rest elements are ignored.
sputs :: Exception e
      => LogStore.Context    -- ^ db context
      -> ByteString          -- ^ topic
      -> Vector ByteString   -- ^ payloads
      -> IO (Either (Vector LogStore.EntryID, e) (Vector LogStore.EntryID))
sputs db topic payloads = do
  rs <- V.mapM (sput db topic) payloads
  case V.findIndex isLeft rs of
    Just i  ->
      let xs = V.unsafeSlice 0 i rs
       in return $ Left (V.map fromRight' xs, fromLeft'(V.unsafeIndex rs i))
    Nothing -> return $ Right $ V.map fromRight' rs

-------------------------------------------------------------------------------
-- Log-store

type Element = (LogStore.Entry, LogStore.EntryID)

readEntry :: MonadIO m
          => LogStore.Context
          -> ByteString
          -> Maybe LogStore.EntryID
          -> Maybe LogStore.EntryID
          -> m (Serial Element)
readEntry db topic start end = runReaderT f db
  where
    f = LogStore.open key ropts >>= \hd -> LogStore.readEntries hd start end
    key = bs2str topic
    ropts = LogStore.defaultOpenOptions

readEntries :: LogStore.Context
            -> ByteString
            -> Maybe LogStore.EntryID
            -> Maybe LogStore.EntryID
            -> IO [Element]
readEntries db topic start end =
  S.toList . serially =<< readEntry db topic start end

appendEntry :: MonadIO m
            => LogStore.Context
            -> ByteString
            -> ByteString
            -> m (LogStore.EntryID)
appendEntry db topic payload = runReaderT f db
  where
    f = LogStore.open key wopts >>= flip LogStore.appendEntry payload
    key = bs2str topic
    wopts = LogStore.defaultOpenOptions { LogStore.writeMode       = True
                                        , LogStore.createIfMissing = True
                                        }

-------------------------------------------------------------------------------

fromLeft' :: Either a b -> a
fromLeft' = fromLeft (error "this should never happen")

fromRight' :: Either a b -> b
fromRight' = fromRight (error "this should never happen")
