-- | A stream data structure based on Streamly.
module Mangrove.Streaming
  ( SerialT
  , Serial
  , take
  , drop
  , splitAt
  , toList
  , encodeFromChunks
  , encodeFromChunksIO
  ) where

import           Data.ByteString                   (ByteString)
import qualified Data.ByteString.Lazy              as BSL
import           Prelude                           hiding (drop, splitAt, take)
import           Streamly                          (Serial, SerialT)
import qualified Streamly.External.ByteString      as S
import qualified Streamly.External.ByteString.Lazy as SL
import qualified Streamly.Prelude                  as S

-------------------------------------------------------------------------------

take :: (Monad m) => Int -> SerialT m a -> SerialT m a
take = S.take

drop :: (Monad m) => Int -> SerialT m a -> SerialT m a
drop = S.drop

splitAt :: (Monad m) => Int -> SerialT m a -> (SerialT m a, SerialT m a)
splitAt n s = (take n s, drop n s)

toList :: (Monad m) => SerialT m a -> m [a]
toList = S.toList

-- | Convert a serial stream of elements to a lazy ByteString.
--
-- __IMPORTANT NOTE__: This function is lazy only for lazy monads (e.g. Identity).
-- For strict monads (e.g. 'IO') it consumes the entire input before generating
-- the output. For 'IO' monad please use 'encodeFromChunksIO' instead.
encodeFromChunks :: Monad m
                 => (a -> ByteString)
                 -> SerialT m a
                 -> m BSL.ByteString
encodeFromChunks enc s = SL.fromChunks $ S.map (S.toArray . enc) s

encodeFromChunksIO :: (a -> ByteString) -> SerialT IO a -> IO BSL.ByteString
encodeFromChunksIO enc s = SL.fromChunksIO $ S.map (S.toArray . enc) s
