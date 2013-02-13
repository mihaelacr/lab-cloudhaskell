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


simpleF :: forall a b . (Serializable a, Serializable b, Show b) => (a -> b) -> WorkStealingArguments -> Process ()
simpleF f = workStealingSlave $ \(x :: a) -> (do
  liftIO $ putStrLn "calculated"
  -- send master (f x)
  -- liftIO $ putStrLn "sent"
  return (f x)
  )  -- :: Process b

simpleFid :: WorkStealingArguments -> Process ()
simpleFid = simpleF (\x -> 2 * (x :: Int))


remotable ['slave, 'simpleFid]


-- | What the master shall do.
master :: Backend -> [NodeId] -> Process ()
master backend slaves = do
  -- Start off worker slaves handling (forks off a process)
  forkWorkStealingMaster slaveProcess work slaves

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


interactive :: IO [Int]
interactive = do
  cloudMap [1,2,3,4::Int] $(mkClosure 'simpleFid)



-- cloudMap :: (Serializable a, Serializable b) => [a] -> (a -> b) -> IO [b]
cloudMap :: forall a b . (Serializable a, Serializable b, Show b) => [a] -> (WorkStealingArguments -> Closure (Process ())) -> IO [b]
cloudMap xs f = do
  host <- getHostName
  backend <- initializeBackend host "0" rtable
  print "h1"
  resultListRef <- newIORef []
  startMaster backend (master' backend resultListRef)
  readIORef resultListRef
  where
    master' _backend resultListRef slaves = do
      -- Start off worker slaves handling (forks off a process)
      forkWorkStealingMaster slaveProcess xs slaves

      -- Run the code that receives the slaves' answers
      liftIO $ print "here"
      results <- collect (length xs) []
      liftIO $ print "collected"

      liftIO $ writeIORef resultListRef results
      liftIO $ print "written"

      -- terminateAllSlaves backend

      where
        -- What to start on the slaves
        -- slaveProcess = $(mkClosure 'slave)
        slaveProcess = f

        collect 0 ress = return ress
        collect n ress | n > 0 = do
          liftIO $ print "collecting"

          masterPid <- getSelfPid
          liftIO . putStrLn $ "collect PID: " ++ show masterPid

          res <- expect
          liftIO . print $ "collected " ++ show (res :: b)
          collect (n-1) (res:ress)
