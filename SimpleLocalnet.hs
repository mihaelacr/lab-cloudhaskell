{-# LANGUAGE BangPatterns, TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}


import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet
import System.Exit
import System.IO (hPutStrLn, stderr)

import WorkStealing (workStealingMaster, workStealingSlave)





-- | An example "reduce" function: Wait for n integers and sum them all up
sumIntegers :: Int -> Process Integer
sumIntegers = go 0
  where
    go :: Integer -> Int -> Process Integer
    go !acc 0 = return acc
    go !acc n = do
      m <- expect
      go (acc + m) (n - 1)


slaveWorker :: (ProcessId, ProcessId) -> Process ()
slaveWorker = workStealingSlave $ \master (x :: Integer) -> do
  send master "some extra message"
  return (x*2)


remotable ['slaveWorker]


master :: Backend -> [NodeId] -> Process ()
master backend slaves = do
  result <- workStealingMaster slaveProcess work resultProcess slaves

  liftIO $ print result

  -- Terminate the slaves when the master terminates (this is optional)
  terminateAllSlaves backend

  where
    slaveProcess = $(mkClosure 'slaveWorker)
    n = 10
    work = [1..n] :: [Integer]
    resultProcess = sumIntegers (fromIntegral n)



rtable :: RemoteTable
rtable = __remoteTable initRemoteTable



main :: IO ()
main = do
  args <- getArgs

  case args of
    ["master", host, port] -> do
      backend <- initializeBackend host port rtable
      startMaster backend (master backend)
    ["slave", host, port] -> do
      -- host <- getHostName
      backend <- initializeBackend host port rtable
      -- This does terminate only when terminateSlave / terminateAllSlaves is called
      startSlave backend
    _ -> do
      hPutStrLn stderr "invalid arguments" >> exitFailure
