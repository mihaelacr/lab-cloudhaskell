{-# LANGUAGE BangPatterns, TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}

module WorkStealing where

import Control.Monad
import Control.Distributed.Process
import Control.Distributed.Process.Closure
-- import PrimeFactors

slave :: (ProcessId, ProcessId) -> Process ()
slave (master, workQueue) = do
    us <- getSelfPid
    liftIO . print $ "jo"
    go us
  where
    go us = do
      -- Ask the queue for work
      send workQueue us
      liftIO . print $ "sent"

      -- If there is work, do it, otherwise terminate
      receiveWait
        [ match $ \(n :: Int)  -> do
                            liftIO . print $ ("matched", n)
                            send master ("hello " ++ show n)
                            liftIO . print $ ("matched sent", n)
                            go us
        , match $ \() -> return ()
        , matchUnknown $ do
                            liftIO . putStrLn $ "WARNING: Unknown message received"
                            go us
        -- , match $ \_  -> liftIO . print $ "whatever"
        ]

remotable ['slave]

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

    -- Once all the work is done, tell the slaves to terminate
    forever $ do
      pid <- expect
      send pid ()

  -- Start slave processes
  forM_ slaves $ \nid -> spawn nid ($(mkClosure 'slave) (us, workQueue))

  -- Wait for the result
  sumIntegers (fromIntegral n)
