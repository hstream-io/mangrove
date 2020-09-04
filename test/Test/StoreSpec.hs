{-# LANGUAGE OverloadedStrings #-}

module Test.StoreSpec (spec) where

import           Control.Exception    (bracket, SomeException)
import           Control.Monad.Reader (runReaderT)
import           Data.ByteString      (ByteString)
import qualified Data.ByteString      as BS
import           Data.Sequence        (Seq)
import qualified Data.Sequence        as Seq
import           Data.Word            (Word64, Word32)
import           System.IO.Temp       (withSystemTempDirectory)
import           Test.Hspec

import qualified Log.Store.Base       as Store
import           Mangrove.Store       (sgetAll, sput)

spec :: Spec
spec = do
  streamPutGet

defaultWriteBufferSize :: Word64
defaultWriteBufferSize = 64 * 1024 * 1024

defaultEnableDBStats :: Bool
defaultEnableDBStats = True

defaultDBStatsPeriodSec :: Word32
defaultDBStatsPeriodSec = 10

defaultPartitionInterval :: Int
defaultPartitionInterval = 60

defaultPartitionFileNumLimit :: Int
defaultPartitionFileNumLimit = 16

defaultMaxOpenDBs :: Int
defaultMaxOpenDBs = -1

streamPutGet :: Spec
streamPutGet = describe "Stream Put Get operations" $ do
  it "put one element to db and then get it out" $ do
    withSystemTempDirectory "rocksdb-test" $ \dbdir -> do
      bracket
        (Store.initialize $
          Store.Config dbdir defaultWriteBufferSize defaultEnableDBStats
            defaultDBStatsPeriodSec defaultPartitionInterval
            defaultPartitionFileNumLimit defaultMaxOpenDBs)
        (runReaderT Store.shutDown)
        putget `shouldReturn` True

putget :: Store.Context -> IO Bool
putget ctx = do
  r <- sput ctx topic payload :: IO (Either SomeException Store.EntryID)
  case r of
    Left _        -> return False
    Right entryid -> do
      r' <- sgetAll ctx topic (Just entryid) Nothing 10 0 :: IO (Either SomeException (Seq (Store.EntryID, Store.Entry)))
      case r' of
        Right ((entryid', entry) Seq.:<| _) ->
          return $ entryid' == entryid && entry == payload
        _ -> return False

topic :: ByteString
topic = "test_topic"

payload :: ByteString
payload = BS.concat $ replicate 10 "foo"
