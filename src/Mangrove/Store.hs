{-# LANGUAGE OverloadedStrings #-}

module Mangrove.Store
  ( sget
  , sput
  ) where

import           Conduit                   (ConduitT, runConduit, sinkList,
                                            (.|))
import           Control.Monad.IO.Class    (MonadIO)
import           Control.Monad.Reader      (runReaderT)
import           Control.Monad.Trans.Maybe (runMaybeT)
import           Data.ByteString           (ByteString)

import qualified Log.Store.Base            as LogStore
import           Mangrove.Utils            (bs2str)

-------------------------------------------------------------------------------

-- | Get a "snapshot" of elements from a stream.
sget :: LogStore.Context          -- ^ db context
     -> ByteString                -- ^ topic
     -> Maybe LogStore.EntryID    -- ^ start entry id
     -> Maybe LogStore.EntryID    -- ^ end entry id
     -> Integer                   -- ^ offset
     -> Integer                   -- ^ max size
     -> IO (Either String [Element])
sget db topic startId endId offset maxn = do
  m_s <- readEntries db topic startId endId
  case m_s of
    Nothing -> return $ Left "db error"
    Just s  -> do
      xs <- snapshot s offset maxn
      case sequence xs of
        Nothing -> return $ Left "db return nothing for some ids"
        Just rs -> return $ Right rs

-- | Put element to a stream.
sput :: LogStore.Context    -- ^ db context
     -> ByteString          -- ^ topic
     -> ByteString          -- ^ payload
     -> IO (Either String LogStore.EntryID)
sput db topic payload = do
  r <- appendEntry db topic payload
  case r of
    Nothing      -> return $ Left "db set error"
    Just entryid -> return $ Right entryid

-------------------------------------------------------------------------------

type Element = (LogStore.Entry, LogStore.EntryID)

type Stream a = ConduitT () a IO ()

snapshot :: Stream a -> Integer -> Integer -> IO [a]
snapshot source offset maxn = do
  rs <- runConduit $ source .| sinkList
  return $ take (fromIntegral maxn) $ drop (fromIntegral offset) rs

-------------------------------------------------------------------------------
-- Log-store

readEntries :: MonadIO m
            => LogStore.Context
            -> ByteString
            -> Maybe LogStore.EntryID
            -> Maybe LogStore.EntryID
            -> m (Maybe (Stream (Maybe Element)))
readEntries db topic startId endId = do
  let ropts = LogStore.defaultOpenOptions { LogStore.writeMode       = True
                                          , LogStore.createIfMissing = True
                                          }
  runMaybeT $ runReaderT
    (do lh <- LogStore.open (bs2str topic) ropts
        LogStore.readEntries lh startId endId
    )
    db

appendEntry :: MonadIO m
            => LogStore.Context
            -> ByteString
            -> ByteString
            -> m (Maybe LogStore.EntryID)
appendEntry db topic payload = do
  let wopts = LogStore.defaultOpenOptions { LogStore.writeMode       = True
                                          , LogStore.createIfMissing = True
                                          }
  runMaybeT $ runReaderT
    (do lh <- LogStore.open (bs2str topic) wopts
        LogStore.appendEntry lh payload
    )
    db
