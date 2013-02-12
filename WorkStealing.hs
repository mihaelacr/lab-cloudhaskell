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


slave :: forall a b . (Serializable a, Serializable b) => (ProcessId -> a -> Process b) -> (ProcessId, ProcessId) -> Process ()
slave slaveProcess (master, workQueue) = do
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


slaveX :: (ProcessId, ProcessId) -> Process ()
slaveX = slave $ \master (x :: Integer) -> do
  send master "some extra message"
  return (x*2)

-- remotable ['slave]
remotable ['slaveX]

-- | Wait for n integers and sum them all up
sumIntegers :: Int -> Process Integer
sumIntegers = go 0
  where
    go :: Integer -> Int -> Process Integer
    go !acc 0 = return acc
    go !acc n = do
      m <- expect
      go (acc + m) (n - 1)

master :: Integer -> [NodeId] -> Process Integer
master n slaves = do
  us <- getSelfPid

  workQueue :: ProcessId <- spawnLocal $ do
    -- Reply with the next bit of work to be done
    forM_ [1 .. n] $ \(m :: Integer) -> do
      them <- expect
      liftIO . print $ ("got them", them)
      send them m
      liftIO . print $ ("sent them", them, m)

    liftIO $ putStrLn "loop done"

    -- Once all the work is done, tell the slaves to terminate
    forever $ do
      pid <- expect
      send pid NoMoreWork

  -- Start slave processes
  forM_ slaves $ \nid -> spawn nid ($(mkClosure 'slaveX) (us, workQueue))

  -- Wait for the result
  sumIntegers (fromIntegral n)


