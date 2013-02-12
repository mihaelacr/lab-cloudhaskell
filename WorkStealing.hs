{-# LANGUAGE BangPatterns, TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}

module WorkStealing where

import Control.Monad
import Control.Distributed.Process
import Control.Distributed.Process.Closure
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


workStealingSlave :: forall a b . (Serializable a, Serializable b) =>
                                  (ProcessId -> a -> Process b)
                               -> (ProcessId, ProcessId)
                               -> Process ()
workStealingSlave slaveProcess (master, workQueue) = do
    us <- getSelfPid
    logSlave "INITIALIZED"
    run us
    logSlave "DONE FOR THIS MASTER"
  where
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
    logSlave s = liftIO . putStrLn $ "Work stealing slave: " ++ s



workStealingMaster :: (Serializable work) =>
                      ((ProcessId, ProcessId) -> Closure (Process ()))
                   -> [work]
                   -> Process result
                   -> [NodeId]
                   -> Process result
workStealingMaster slaveProcess work resultProcess slaves = do
  masterPid <- getSelfPid

  -- Make a working queue process that handles assigning work to ready slaves
  queue <- spawnLocal workQueue

  -- Start slave processes on the slaves (asynchronous)
  forM_ slaves $ \nid -> spawn nid (slaveProcess (masterPid, queue))

  -- Run the code that receives the slaves' answers
  resultProcess

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



-- | An example "reduce" function: Wait for n integers and sum them all up
sumIntegers :: Int -> Process Integer
sumIntegers = go 0
  where
    go :: Integer -> Int -> Process Integer
    go !acc 0 = return acc
    go !acc n = do
      m <- expect
      go (acc + m) (n - 1)


slave :: (ProcessId, ProcessId) -> Process ()
slave = workStealingSlave $ \master (x :: Integer) -> do
  send master "some extra message"
  return (x*2)


remotable ['slave]


master :: Integer -> [NodeId] -> Process Integer
master n slaves = workStealingMaster slaveProcess work resultProcess slaves
  where
    slaveProcess = $(mkClosure 'slave)
    work = [1..n] :: [Integer]
    resultProcess = sumIntegers (fromIntegral n)
