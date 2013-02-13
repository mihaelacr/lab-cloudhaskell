{-# LANGUAGE ScopedTypeVariables, DeriveDataTypeable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module WorkStealing where

import Control.Monad
import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Data.Typeable
import Control.Concurrent.Chan as Chan
import Data.Binary


data WorkStealingControlMessage = NoMoreWork
                                deriving (Typeable)


instance Binary WorkStealingControlMessage where
  put NoMoreWork = putWord8 0
  get = do
    header <- getWord8
    case header of
      0 -> return NoMoreWork
      _ -> fail "WorkStealingControlMessage.get: invalid"


-- newtype WorkStealingArguments = WorkStealingArguments (ProcessId, ProcessId)
--                               deriving (Typeable, Binary)
type WorkStealingArguments = (ProcessId, ProcessId)


workStealingSlave :: forall a b . (Serializable a, Serializable b, Show b) =>
                                  (a -> Process b)
                               -> WorkStealingArguments -> Process ()
-- workStealingSlave slaveProcess (WorkStealingArguments (master, workQueue)) = do
workStealingSlave slaveProcess (master, workQueue) = do
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

      -- If there is work, do it
      receiveWait (
        -- [ match $ \(x :: a) -> (slaveProcess master x >>= send master) >> run us
        [ match $ \(x :: a) -> do
                                  res <- slaveProcess x
                                  send master res
                                  liftIO . print $ ("res", res)
                                  run us
        , match $ \NoMoreWork -> return ()
        , matchUnknown $ do
                            logSlave "WARNING: Unknown message received"
                            run us
        ]
        )


-- | Sets up a master for work pushing.
-- Forks off a process that manages a work queue.
forkWorkStealingMaster :: forall a . (Serializable a) =>
                                     ((ProcessId, ProcessId) -> Closure (Process ()))
                                  -> Chan a
                                  -> [NodeId]
                                  -- -> Process (SendPort a)
                                  -> Process ()
forkWorkStealingMaster slaveProcess queueChan slaves = do
  masterPid <- getSelfPid
  logMaster $ "forkWorkStealingMaster PID: " ++ show masterPid

  -- TODO create typed channel here instead of manual send/expect
  -- (queueInputChan, queueReceiveChan) <- newChan
  -- queueChan <- liftIO Chan.newChan

  -- Make a working queue process that handles assigning work to ready slaves
  -- queuePid <- spawnLocal (workQueue queueReceiveChan)
  queuePid <- spawnLocal (workQueue queueChan)

  -- Start slave processes on the slaves (asynchronous)
  forM_ slaves $ \nid -> spawn nid (slaveProcess (masterPid, queuePid))

  -- return queueInputChan
  -- return queueChan

  where
    logMaster s = liftIO . putStrLn $ "Work stealing master: " ++ s

    workQueue :: (Serializable a) => Chan a -> Process ()
    workQueue queueReceiveChan = do
      -- Reply with the next bit of work to be done
      _ <- forever $ do
        slavePid <- expect
        logMaster $ "SLAVE ANNOUNCED READYNESS: " ++ show slavePid
        workUnit <- liftIO $ readChan queueReceiveChan
        logMaster $ "got receiveChan work"
        send slavePid workUnit
        logMaster $ "SENT WORK TO " ++ show slavePid

      -- logMaster "ALL WORK DONE, NOTIFYING SLAVES OF WORK DONE"
      logMaster "ALL WORK DONE, WORK QUEUE TERMINATING"

      -- Once all the work is done, tell the slaves that there is no more work
      -- This does not terminate
      -- forever $ do
      --   pid <- expect
      --   send pid NoMoreWork
