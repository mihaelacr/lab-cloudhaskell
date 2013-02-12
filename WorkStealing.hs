{-# LANGUAGE ScopedTypeVariables, DeriveDataTypeable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module WorkStealing where

import Control.Monad
import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Data.Typeable
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


newtype WorkStealingArguments = WorkStealingArguments (ProcessId, ProcessId)
                              deriving (Typeable, Binary)


workStealingSlave :: forall a b . (Serializable a, Serializable b) =>
                                  (ProcessId -> a -> Process b)
                               -> WorkStealingArguments
                               -> Process ()
workStealingSlave slaveProcess (WorkStealingArguments (master, workQueue)) = do
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
        [ match $ \(x :: a) -> (slaveProcess master x >>= send master) >> run us
        , match $ \NoMoreWork -> return ()
        , matchUnknown $ do
                            logSlave "WARNING: Unknown message received"
                            run us
        ]
        )


-- | Sets up a master for work pushing.
-- Forks off a process that manages a work queue.
forkWorkStealingMaster :: (Serializable work) =>
                          ((ProcessId, ProcessId) -> Closure (Process ()))
                       -> [work]
                       -> [NodeId]
                       -> Process ()
forkWorkStealingMaster slaveProcess work slaves = do
  masterPid <- getSelfPid

  -- Make a working queue process that handles assigning work to ready slaves
  queue <- spawnLocal workQueue

  -- Start slave processes on the slaves (asynchronous)
  forM_ slaves $ \nid -> spawn nid (slaveProcess (masterPid, queue))

  where
    logMaster s = liftIO . putStrLn $ "Work stealing master: " ++ s

    workQueue :: Process ()
    workQueue = do
      -- Reply with the next bit of work to be done
      forM_ work $ \workUnit -> do
        slavePid <- expect
        logMaster $ "SLAVE ANNOUNCED " ++ show slavePid ++ " READYNESS"
        send slavePid workUnit
        logMaster $ "SENT WORK TO " ++ show slavePid

      logMaster "ALL WORK DONE, NOTIFYING SLAVES OF WORK DONE"

      -- Once all the work is done, tell the slaves that there is no more work
      -- This does not terminate
      forever $ do
        pid <- expect
        send pid NoMoreWork
