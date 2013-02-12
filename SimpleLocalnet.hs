{-# LANGUAGE BangPatterns, TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}


import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet
import System.Exit
import System.IO (hPutStrLn, stderr)

import WorkStealing (forkWorkStealingMaster, workStealingSlave)



-- | An example "reduce" function: Wait for n integers and sum them all up
sumIntegers :: Int -> Process Integer
sumIntegers = go 0
  where
    go :: Integer -> Int -> Process Integer
    go !acc 0 = return acc
    go !acc n = do
      m <- expect
      go (acc + m) (n - 1)


-- | What a slave shall do.
-- Is given a piece of work and the process ID of the master.
slave :: (ProcessId, ProcessId) -> Process ()
slave = workStealingSlave $ \master (x :: Integer) -> do
  send master "some extra message"
  return (x*2)

-- Set up 'slave' as a remote function.
remotable ['slave]


-- | What the master shall do.
master :: Backend -> [NodeId] -> Process ()
master backend slaves = do
  -- Start off worker slaves handling (forks off a process)
  forkWorkStealingMaster slaveProcess work slaves

  -- Run the code that receives the slaves' answers
  result <- sumIntegers (fromIntegral n)

  liftIO . print $ result

  -- Terminate the slaves when the master terminates (this is optional)
  terminateAllSlaves backend

  where
    -- What to start on the slaves
    slaveProcess = $(mkClosure 'slave)
    n = 10
    work = [1..n] :: [Integer]



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
      -- This does terminate only when terminateSlave / terminateAllSlaves is called from the master
      startSlave backend
    _ -> do
      hPutStrLn stderr "invalid arguments" >> exitFailure
