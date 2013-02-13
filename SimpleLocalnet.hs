{-# LANGUAGE BangPatterns, TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}


import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Serializable
import Control.Monad
import System.Exit
import System.IO (hPutStrLn, stderr)
import Network.HostName (getHostName)
import Data.IORef
import Control.Concurrent.Chan as Chan

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


setUp :: forall a b . (Serializable a, Serializable b, Show a) => (WorkStealingArguments -> Closure (Process ())) -> IO (Chan a, Chan b)
setUp remoteClosure = do
  host <- getHostName

  backend <- initializeBackend host "0" rtable

  inChan <- Chan.newChan
  outChan <- Chan.newChan

  startMaster backend (master' backend inChan outChan)

  return (inChan, outChan)

  where
    master' _backend inChan _outChan slaves = do
      -- Start off worker slaves handling (forks off a process)
      workInputChan <- forkWorkStealingMaster remoteClosure inChan slaves

      -- Start off result receival
      -- spawnLocal $ do
      --   forever $ do
      --     res :: b <- expect
      --     liftIO $ writeChan outChan res

      liftIO . putStrLn $ "spawned receival"

      -- Loop forever in sending work
      -- void $ spawnLocal $ forever $ do
      --   liftIO . putStrLn $ "waiting for inChan input"
      --   work <- liftIO $ readChan inChan
      --   liftIO . putStrLn $ "sending work " ++ show work
      --   sendChan workInputChan work
      --   liftIO . putStrLn $ "sent to workInputChan"


remoteDoubleClosure :: WorkStealingArguments -> Closure (Process ())
remoteDoubleClosure = $(mkClosure 'remoteDouble)


main2 = do
  (inChan, outChan) <- setUp remoteDoubleClosure
  r :: [Int] <- cloudMap inChan outChan [1..4::Int]
  return r


-- | What the master shall do.
master :: Backend -> [NodeId] -> Process ()
master backend slaves = do
  -- Start off worker slaves handling (forks off a process)
  -- workInputChan <- forkWorkStealingMaster slaveProcess undefined slaves
  forkWorkStealingMaster slaveProcess (undefined :: Chan Int) slaves

  -- mapM (sendChan workInputChan) work

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
cloudMap :: forall a b . (Serializable a, Serializable b, Show b) => Chan a -> Chan b -> [a] -> IO [b]
cloudMap workInputChan outChan xs = do
    (liftIO $ putStrLn "asdf3")

    mapM (writeChan workInputChan) xs

    (liftIO $ putStrLn "asdf4")
    -- Run the code that receives the slaves' answers
    results <- collect (length xs) []
    (liftIO $ putStrLn "asdf5")

    -- liftIO $ writeIORef resultListRef results

    -- terminateAllSlaves backend
    return results

    where
      collect 0 ress = return ress
      collect n ress | n > 0 = do
        res <- readChan outChan
        collect (n-1) (res:ress)
