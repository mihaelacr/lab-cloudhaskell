module WorkStealingMain where

import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Backend.SimpleLocalnet
import System.Exit
import Network.HostName (getHostName)
import System.IO (hPutStrLn, stderr)


workStealingMain :: RemoteTable -> IO () -> IO ()
workStealingMain rtable masterMain = do
  args <- getArgs

  case args of
    ["master"] -> masterMain

    ["slave", host, port] -> do
      backend <- initializeBackend host port rtable
      -- This does terminate only when terminateSlave / terminateAllSlaves is called from the master
      startSlave backend

    _ -> do
      hPutStrLn stderr "invalid arguments" >> exitFailure


mkAutoBackend :: RemoteTable -> IO Backend
mkAutoBackend rtable = do
  host <- getHostName
  initializeBackend host "0" rtable
