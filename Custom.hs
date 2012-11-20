{-# LANGUAGE DeriveDataTypeable #-}

import Control.Applicative
import Control.Monad
import Data.Maybe
import Control.Concurrent (forkIO, threadDelay, ThreadId)
import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Node (initRemoteTable, newLocalNode)
import Control.Distributed.Process.Backend.SimpleLocalnet hiding (newLocalNode, startSlave, terminateSlave, findSlaves, terminateAllSlaves)

import Data.Binary (Binary(get, put), getWord8, putWord8)
import Data.Typeable (Typeable)

import qualified Network.Transport.TCP as NT
  ( createTransport
  , defaultTCPParameters
  )

import qualified Control.Distributed.Process.Node as Node
  ( LocalNode
  , newLocalNode
  , localNodeId
  , runProcess
  )

import qualified WorkStealing

rtable :: RemoteTable
rtable = WorkStealing.__remoteTable initRemoteTable


master :: Backend -> [NodeId] -> Process ()
master backend slaves = do
  -- Do something interesting with the slaves
  liftIO . putStrLn $ "Slaves: " ++ show slaves
  -- Terminate the slaves when the master terminates (this is ptional)
  terminateAllSlaves backend

main :: IO ()
main = do
  args <- getArgs

  case args of
    ["master", host, port] -> do
      trans <- gimmeTrans host port
      -- startMaster backend (master backend)
      undefined
    ["slave", host, port] -> do
      trans <- gimmeTrans host port
      node <- newLocalNode trans rtable
      Node.runProcess node slaveController

gimmeTrans host port = do
  mTransport <- NT.createTransport host port NT.defaultTCPParameters

  case mTransport of
    Left _ -> error "no transport"
    Right x -> return x






data SlaveControllerMsg =
    SlaveTerminate
  deriving (Typeable, Show)

instance Binary SlaveControllerMsg where
  put SlaveTerminate = putWord8 0
  get = do
    header <- getWord8
    case header of
      0 -> return SlaveTerminate
      _ -> fail "SlaveControllerMsg.get: invalid"


-- | The slave controller interprets 'SlaveControllerMsg's
slaveController :: Process ()
slaveController = do
    pid <- getSelfPid
    register "slaveController" pid
    go
  where
    go = do
      msg <- expect
      case msg of
        SlaveTerminate -> return ()


-- | Terminate the slave at the given node ID
terminateSlave :: NodeId -> Process ()
terminateSlave nid = nsendRemote nid "slaveController" SlaveTerminate

-- | Find slave nodes
findSlaves :: Backend -> Process [NodeId]
findSlaves backend = do
  nodes <- liftIO $ findPeers backend 1000000
  -- Fire of asynchronous requests for the slave controller
  forM_ nodes $ \nid -> whereisRemoteAsync nid "slaveController"
  -- Wait for the replies
  catMaybes <$> forM nodes (\_ ->
    receiveWait
      [ matchIf (\(WhereIsReply label _) -> label == "slaveController")
                (\(WhereIsReply _ mPid) -> return (processNodeId <$> mPid))
      ])

-- | Terminate all slaves
terminateAllSlaves :: Backend -> Process ()
terminateAllSlaves backend = do
  slaves <- findSlaves backend
  forM_ slaves terminateSlave
  liftIO $ threadDelay 1000000


-- | 'startMaster' finds all slaves currently available on the local network
-- (which should therefore be started first), redirects all log messages to
-- itself, and then calls the specified process, passing the list of slaves
-- nodes.
--
-- Terminates when the specified process terminates. If you want to terminate
-- the slaves when the master terminates, you should manually call
-- 'terminateAllSlaves'.
-- startMaster :: Backend -> ([NodeId] -> Process ()) -> IO ()
-- startMaster backend proc = do
--   node <- newLocalNode backend
--   Node.runProcess node $ do
--     slaves <- findSlaves backend
--     redirectLogsHere backend
--     proc slaves


-- | Find slave nodes
findSlaves :: Backend -> Process [NodeId]
findSlaves backend = do
  nodes <- liftIO $ findPeers backend 1000000
  -- Fire of asynchronous requests for the slave controller
  forM_ nodes $ \nid -> whereisRemoteAsync nid "slaveController"
  -- Wait for the replies
  catMaybes <$> forM nodes (\_ ->
    receiveWait
      [ matchIf (\(WhereIsReply label _) -> label == "slaveController")
                (\(WhereIsReply _ mPid) -> return (processNodeId <$> mPid))
      ])
