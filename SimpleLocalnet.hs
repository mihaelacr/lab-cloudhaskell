{-# LANGUAGE BangPatterns, TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}


import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Serializable
import System.Exit
import System.IO (hPutStrLn, stderr)
import Network.HostName (getHostName)
import Data.IORef

import WorkStealing (WorkStealingArguments, forkWorkStealingMaster, workStealingSlave)



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
slave :: WorkStealingArguments -> Process ()
slave = workStealingSlave $ \(x :: Integer) -> do
  return (x*2)

-- Set up 'slave' as a remote function.
-- remotable ['slave]


remoteDouble :: WorkStealingArguments -> Process ()
remoteDouble = workStealingSlave $ \(x :: Int) -> return (2 * x)


remotable ['slave, 'remoteDouble]


setUp :: (Serializable a) => (WorkStealingArguments -> Closure (Process ())) -> IO (SendPort a)
setUp remoteClosure = do
  host <- getHostName

  backend <- initializeBackend host "0" rtable

  chanRef <- newIORef undefined

  startMaster backend (master' backend chanRef)

  readIORef chanRef

  where
    master' _backend chanRef slaves = do
      -- Start off worker slaves handling (forks off a process)
      workInputChan <- forkWorkStealingMaster remoteClosure slaves
      liftIO $ writeIORef chanRef workInputChan


remoteDoubleClosure :: WorkStealingArguments -> Closure (Process ())
remoteDoubleClosure = $(mkClosure 'remoteDouble)

interactive :: SendPort Int -> [Int] -> IO [Int]
-- interactive = cloudMap [1,2,3,4::Int] $(mkClosure 'remoteDouble)
interactive workInputChan l = cloudMap workInputChan l



-- | What the master shall do.
master :: Backend -> [NodeId] -> Process ()
master backend slaves = do
  -- Start off worker slaves handling (forks off a process)
  workInputChan <- forkWorkStealingMaster slaveProcess slaves

  mapM (sendChan workInputChan) work

  -- Run the code that receives the slaves' answers
  result <- sumIntegers (fromIntegral n)

  liftIO . print $ result

  -- Terminate the slaves when the master terminates (this is optional)
  -- terminateAllSlaves backend

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
      backend <- initializeBackend host port rtable
      -- This does terminate only when terminateSlave / terminateAllSlaves is called from the master
      startSlave backend
    _ -> do
      hPutStrLn stderr "invalid arguments" >> exitFailure


-- cloudMap :: (Serializable a, Serializable b) => [a] -> (a -> b) -> IO [b]
cloudMap :: forall a b . (Serializable a, Serializable b, Show b) => SendPort a -> [a] -> IO [b]
cloudMap workInputChan xs = do
    (liftIO $ putStrLn "asdf3")

    mapM (sendChan workInputChan) xs

    (liftIO $ putStrLn "asdf4")
    -- Run the code that receives the slaves' answers
    results <- collect (length xs) []
    (liftIO $ putStrLn "asdf5")

    -- liftIO $ writeIORef resultListRef results

    -- terminateAllSlaves backend
    terminate

    where
      collect 0 ress = return ress
      collect n ress | n > 0 = do
        res <- expect
        collect (n-1) (res:ress)
