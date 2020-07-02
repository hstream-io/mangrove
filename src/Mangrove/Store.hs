{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Mangrove.Store
  ( SgetChunks (..)
  , sget
  , sgetAll

  , sput
  , sputs
  ) where

import           Control.Exception      (Exception, SomeException, try)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.Reader   (runReaderT)
import           Data.ByteString        (ByteString)
import           Data.Either            (fromLeft, fromRight, isLeft)
import           Data.Vector            (Vector)
import qualified Data.Vector            as V
import           Streamly               (Serial)
import qualified Streamly.Prelude       as S

import qualified Log.Store.Base         as LogStore
import           Mangrove.Utils         (bs2str)

-------------------------------------------------------------------------------

data SgetChunks = SgetStep [Element]
                | SgetDone
                | SgetFail SomeException

-- | Get a "snapshot" of elements.
sget :: forall m . MonadIO m
     => LogStore.Context          -- ^ db context
     -> ByteString                -- ^ topic
     -> Maybe LogStore.EntryID    -- ^ start entry id
     -> Maybe LogStore.EntryID    -- ^ end entry id
     -> Int                       -- ^ max chunk size
     -> Int                       -- ^ offset
     -> (SgetChunks -> m ())
     -> m ()
sget db topic start end n offset process = stream >>= go
  where
    stream :: m (Serial Element)
    stream = liftIO $ S.drop offset <$> readEntry db topic start end
    go :: Serial Element -> m ()
    go s = do
      e_trunks <- liftIO $ try . S.toList $ S.take n s
      case e_trunks of
        Left e       -> process $ SgetFail e
        Right trunks ->
          if null trunks
             then process SgetDone
             else process (SgetStep trunks) >> (go $ S.drop n s)

-- | Get a "snapshot" of elements.
--
-- Warning: working on large lists could be very inefficient.
sgetAll :: Exception e
        => LogStore.Context          -- ^ db context
        -> ByteString                -- ^ topic
        -> Maybe LogStore.EntryID    -- ^ start entry id
        -> Maybe LogStore.EntryID    -- ^ end entry id
        -> Int                       -- ^ max size
        -> Int                       -- ^ offset
        -> IO (Either e [Element])
sgetAll db topic start end maxn offset = try $ do
  xs <- readEntries db topic start end
  return $ take maxn . drop offset $ xs

-- | Put an element to a stream.
sput :: Exception e
     => LogStore.Context    -- ^ db context
     -> ByteString          -- ^ topic
     -> ByteString          -- ^ payload
     -> IO (Either e LogStore.EntryID)
sput db topic payload = try $ do
  handle <- openWrite db topic
  appendEntry db handle payload

-- | Put elements to a stream.
--
-- If SomeException happens, the rest elements are ignored.
sputs :: Exception e
      => LogStore.Context    -- ^ db context
      -> ByteString          -- ^ topic
      -> Vector ByteString   -- ^ payloads
      -> IO (Either (Vector LogStore.EntryID, e) (Vector LogStore.EntryID))
sputs db topic payloads = do
  ehandle <- try $ openWrite db topic
  case ehandle of
    Left e -> return $ Left (V.empty, e)
    Right handle -> do
      rs <- V.mapM (try . appendEntry db handle) payloads
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

-- | Read all entries into list.
--
-- Warning: working on large lists could be very inefficient.
readEntries :: LogStore.Context
            -> ByteString
            -> Maybe LogStore.EntryID
            -> Maybe LogStore.EntryID
            -> IO [Element]
readEntries db topic start end = S.toList =<< readEntry db topic start end

appendEntry :: MonadIO m
            => LogStore.Context
            -> LogStore.LogHandle
            -> ByteString
            -> m LogStore.EntryID
appendEntry db handle payload = runReaderT f db
  where
    f = LogStore.appendEntry handle payload

openWrite :: MonadIO m
          => LogStore.Context
          -> ByteString
          -> m LogStore.LogHandle
openWrite db topic = runReaderT (LogStore.open key wopts) db
  where
    key = bs2str topic
    wopts = LogStore.defaultOpenOptions { LogStore.writeMode       = True
                                        , LogStore.createIfMissing = True
                                        }

-------------------------------------------------------------------------------

fromLeft' :: Either a b -> a
fromLeft' = fromLeft (error "this should never happen")

fromRight' :: Either a b -> b
fromRight' = fromRight (error "this should never happen")
