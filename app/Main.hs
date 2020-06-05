{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import           Colog               (richMessageAction)
import           Mangrove.Server     (runServer)
import           Mangrove.Types      (Env (..), ServerConfig (..), serverOpts)
import           Options.Applicative (execParser)

main :: IO ()
main = do
    ServerConfig{..} <- execParser serverOpts
    let env = Env serverPort dbPath richMessageAction
    runServer env
