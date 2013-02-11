import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet
-- import Network.HostName (getHostName)
import System.Exit
import System.IO (hPutStrLn, stderr)
import qualified WorkStealing


rtable :: RemoteTable
rtable = WorkStealing.__remoteTable initRemoteTable


master :: Backend -> [NodeId] -> Process ()
master backend slaves = do
  -- Do something interesting with the slaves
  liftIO . putStrLn $ "Slaves: " ++ show slaves

  result <- WorkStealing.master (10) slaves
  liftIO $ print result

  -- Terminate the slaves when the master terminates (this is optional)
  terminateAllSlaves backend



main :: IO ()
main = do
  args <- getArgs

  case args of
    ["master", host, port] -> do
      backend <- initializeBackend host port rtable
      startMaster backend (master backend)
    ["slave", host, port] -> do
      -- host <- getHostName
      backend <- initializeBackend host port rtable
      startSlave backend
    _ -> do
      hPutStrLn stderr "invalid arguments" >> exitFailure

