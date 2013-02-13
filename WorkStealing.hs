{-# LANGUAGE ScopedTypeVariables, DeriveDataTypeable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module WorkStealing where

import Control.Concurrent.Chan as Chan
import Control.Distributed.Process
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Serializable
import Control.Monad


type WorkStealingArguments = (ProcessId, ProcessId)


workStealingSlave :: forall a b . (Serializable a, Serializable b) =>
                                  (a -> Process b)
                               -> WorkStealingArguments -> Process ()
workStealingSlave slaveProcess (workQueue, resultQueue) = do
    us <- getSelfPid
    logSlave "INITIALIZED"
    run us
    logSlave "DONE FOR THIS MASTER"
  where
    logSlave s = liftIO . putStrLn $ "Work stealing slave: " ++ s

    run us = do
      -- Ask the queue for work
      logSlave "ANNOUNCING MYSELF"
      send workQueue us
      logSlave "WAITING FOR WORK"

      -- TODO should we add back a AllWorkDone message to terminate the slave?
      -- What will happen if we don't, with every master setting up the slaves again?

      -- If there is work, do it
      receiveWait $
        [ match $ \(x :: a) -> (slaveProcess x >>= send resultQueue) >> run us
        , matchUnknown $ do
                            logSlave "WARNING: Unknown message received"
                            run us
        ]


-- | Sets up a master for work pushing.
-- Forks off a process that manages a work queue.
forkWorkStealingMaster :: forall a b . (Serializable a, Serializable b) =>
                                       ((ProcessId, ProcessId) -> Closure (Process ()))
                                    -> Chan a
                                    -> Chan b
                                    -> [NodeId]
                                    -> Process ()
forkWorkStealingMaster slaveProcess workChan resChan slaves = do
  masterPid <- getSelfPid
  logMaster $ "forkWorkStealingMaster PID: " ++ show masterPid

  -- TODO create typed channel here instead of manual send/expect

  -- Make a working queue process that handles assigning work to ready slaves
  queuePid <- spawnLocal workQueue

  -- Start off result receiving queue
  resultQueuePid <- spawnLocal $ do
    forever $ do
      res :: b <- expect
      liftIO $ writeChan resChan res

  -- Start slave processes on the slaves (asynchronous)
  forM_ slaves $ \nid -> spawn nid (slaveProcess (queuePid, resultQueuePid))

  where
    logMaster s = liftIO . putStrLn $ "Work stealing master: " ++ s

    workQueue :: (Serializable a) => Process ()
    workQueue = do
      -- Reply with the next bit of work to be done
      forever $ do
        slavePid <- expect
        logMaster $ "SLAVE ANNOUNCED READYNESS: " ++ show slavePid
        workUnit <- liftIO $ readChan workChan
        send slavePid workUnit
        logMaster $ "SENT WORK TO " ++ show slavePid


-- | setUpRemoteFun can only be used once per application, because it uses startMaster.
-- See https://cloud-haskell.atlassian.net/browse/DPSLN-10.
setUpRemoteFun :: forall a b . (Serializable a, Serializable b, Show a) =>
                               (WorkStealingArguments -> Closure (Process ()))
                            -> Backend
                            -> IO (Chan a, Chan b)
setUpRemoteFun remoteClosure backend = do

  inChan <- Chan.newChan
  outChan <- Chan.newChan

  -- Start off worker slaves handling (forks off a process)
  startMaster backend (forkWorkStealingMaster remoteClosure inChan outChan)

  return (inChan, outChan)


cloudMap :: forall a b . (Serializable a, Serializable b, Show b) => Chan a -> Chan b -> [a] -> IO [b]
cloudMap workInputChan outChan xs = do

  mapM (writeChan workInputChan) xs

  -- Run the code that receives the slaves' answers
  collect (length xs)

  where
    collect n
      | n < 0     = error "cloudMap: cannot collect < 0 elements"
      | otherwise = replicateM n (readChan outChan)
