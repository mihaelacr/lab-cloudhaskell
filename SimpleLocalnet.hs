{-# LANGUAGE TemplateHaskell, ScopedTypeVariables #-}

import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node (initRemoteTable)

import WorkStealing (WorkStealingArguments, workStealingSlave, setUp, cloudMap)
import WorkStealingMain


-- The function to distribute
remoteFunction :: WorkStealingArguments -> Process ()
remoteFunction = workStealingSlave $ \(x :: Int) -> return (2 * x)


-- Set up functions to be called remotely
remotable ['remoteFunction]
rtable :: RemoteTable
rtable = __remoteTable initRemoteTable


-- Run this as:
-- * ./program master
-- * ./program slave [REACHABLE_HOSTNAME] [SOME_PORT]
--
-- We can only set up ONE remote function - it has to switch on what comes in.
main :: IO ()
main = workStealingMain (__remoteTable initRemoteTable) $ do
  (inChan, outChan) <- mkAutoBackend rtable >>= setUp $(mkClosure 'remoteFunction)

  -- Using the remote function, here with a cloudMap
  res :: [Int] <- cloudMap inChan outChan [1..4 :: Int]

  print res
