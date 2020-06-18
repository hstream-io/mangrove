{-# LANGUAGE OverloadedStrings #-}

module Test.StoreSpec (spec) where

import           Control.Exception    (bracket)
import           Control.Exception    (SomeException)
import           Control.Monad.Reader (runReaderT)
import           Data.ByteString      (ByteString)
import qualified Data.ByteString      as BS
import           System.IO.Temp       (withSystemTempDirectory)
import           Test.Hspec

import qualified Log.Store.Base       as Store
import           Mangrove.Store       (sget, sput)

spec :: Spec
spec = do
  streamPutGet

streamPutGet :: Spec
streamPutGet = describe "Stream Put Get operations" $ do
  it "put one element to db and then get it out" $ do
    withSystemTempDirectory "rocksdb-test" $ \dbdir -> do
      bracket
        (Store.initialize $ Store.UserDefinedEnv (Store.Config dbdir))
        (runReaderT Store.shutDown)
        putget `shouldReturn` True

putget :: Store.Context -> IO Bool
putget ctx = do
  r <- (sput ctx topic payload) :: IO (Either SomeException Store.EntryID)
  case r of
    Left _        -> return False
    Right entryid -> do
      r' <- (sget ctx topic (Just entryid) Nothing 0 10) :: IO (Either SomeException [(Store.Entry, Store.EntryID)])
      case r' of
        Right [(entry, entryid')] ->
          return $ entryid' == entryid && entry == payload
        _                         -> return False

topic :: ByteString
topic = "test_topic"

payload :: ByteString
payload = BS.concat $ replicate 10 "foo"
