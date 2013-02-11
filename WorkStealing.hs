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


slave :: [Match ()] -> (ProcessId, ProcessId) -> Process ()
slave matches (master, workQueue) = do
    us <- getSelfPid
    liftIO . print $ "jo"
    go us
    liftIO $ putStrLn "slave done"
  where
    go us = do
      -- Ask the queue for work
      send workQueue us
      liftIO . print $ "sent slave worker ready"

      -- If there is work, do it
      receiveWait (
        matches ++
        [ match $ \NoMoreWork -> return ()
        , matchUnknown $ do
                            liftIO . putStrLn $ "WARNING: Unknown message received"
                            go us
        ]
        )

slaveX :: (ProcessId, ProcessId) -> Process ()
slaveX = slave []

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


