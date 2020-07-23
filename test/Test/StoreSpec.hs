{-# LANGUAGE OverloadedStrings #-}

module Test.StoreSpec (spec) where

import           Control.Exception    (bracket)
import           Control.Exception    (SomeException)
import           Control.Monad.Reader (runReaderT)
import           Data.ByteString      (ByteString)
import qualified Data.ByteString      as BS
import           Data.Word            (Word64, Word32)
import           System.IO.Temp       (withSystemTempDirectory)
import           Test.Hspec

import qualified Log.Store.Base       as Store
import           Mangrove.Store       (sgetAll, sput)

spec :: Spec
spec = do
  streamPutGet

defaultCfWriteBufferSize :: Word64
defaultCfWriteBufferSize = 64 * 1024 * 1024

defaultDbWriteBufferSize :: Word64
defaultDbWriteBufferSize = 0

defaultEnableDBStats :: Bool
defaultEnableDBStats = True

defaultDBStatsPeriodSec :: Word32
defaultDBStatsPeriodSec = 10

streamPutGet :: Spec
streamPutGet = describe "Stream Put Get operations" $ do
  it "put one element to db and then get it out" $ do
    withSystemTempDirectory "rocksdb-test" $ \dbdir -> do
      bracket
        (Store.initialize $
          Store.Config dbdir defaultCfWriteBufferSize defaultDbWriteBufferSize
            defaultEnableDBStats defaultDBStatsPeriodSec)
        (runReaderT Store.shutDown)
        putget `shouldReturn` True

putget :: Store.Context -> IO Bool
putget ctx = do
  r <- (sput ctx topic payload) :: IO (Either SomeException Store.EntryID)
  case r of
    Left _        -> return False
    Right entryid -> do
      r' <- (sgetAll ctx topic (Just entryid) Nothing 10 0) :: IO (Either SomeException [(Store.Entry, Store.EntryID)])
      case r' of
        Right [(entry, entryid')] ->
          return $ entryid' == entryid && entry == payload
        _                         -> return False

topic :: ByteString
topic = "test_topic"

payload :: ByteString
payload = BS.concat $ replicate 10 "foo"
