{-# LANGUAGE OverloadedStrings, BangPatterns #-}
module Database.KyotoTycoon ( 
                             -- * Connecting to the database
                             KyotoServer
                            , connect
                            , disconnect
                            , reconnect
                            , cloneConnection
                            , KTResult
                            -- * Basic commands
                            , get
                            , set
                            , remove                             
                            ) where

-- We need ByteStrings a lot here
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.ByteString.Char8 (ByteString)
-- Networking stuff
import Network.Socket (HostName, Socket, SocketOption(..))
import qualified Network.Socket as Sock
import Network.Socket.ByteString
-- Parsing incrementally
import Data.Attoparsec
import qualified Data.Attoparsec.Char8 as P8
-- All ByteString aggregation happens with Builders
import Blaze.ByteString.Builder
import Blaze.ByteString.Builder.Char8 (fromString, fromChar)
-- Miscellaneous imports
import Control.Applicative hiding (many)
import Data.Monoid
import Data.List (foldl')

import Database.KyotoTycoon.TSVRPC

--------------------
-- Server connection
--------------------

-- | Connection to a Kyoto Tyrant server. Accesses are not thread-safe, so if
-- you want to use it in a multithreaded program, you will have to either wrap
-- it in an 'MVar' or use one connection per thread. That is actually a pretty
-- good option, since Kyoto Cabinet is good at dealing with large numbers of
-- concurrent open connections, and it probably will not strain the Haskell
-- runtime.
data KyotoServer = KyotoServer { kt_host   :: ByteString
                               , kt_port   :: ByteString
                               , kt_socket :: Socket
                               }
   deriving Show

-- | Connect to a Kyoto Tycoon server. Arguments are host and port.
connect :: HostName -> String -> IO KyotoServer
connect host port = do
  addrInfo <- Sock.getAddrInfo Nothing (Just host) (Just port)
  let serverAddr = head addrInfo
  sock <- Sock.socket (Sock.addrFamily serverAddr) Sock.Stream Sock.defaultProtocol
  Sock.setSocketOption sock KeepAlive 1
  Sock.connect sock (Sock.addrAddress serverAddr)
  return $ KyotoServer (B.pack host) (B.pack port) sock

-- | Close a Kyoto Tycoon connection.
disconnect :: KyotoServer -> IO ()
disconnect = Sock.sClose . kt_socket

-- | Close a Kyoto Tycoon connection, and return a new one to the same server.
reconnect :: KyotoServer -> IO KyotoServer
reconnect conn = disconnect conn >> cloneConnection conn

-- | Open and return a new connection to the same server as an existing
-- connection. The existing connection need not be open.
cloneConnection :: KyotoServer -> IO KyotoServer
cloneConnection conn = connect (B.unpack $ kt_host conn) (B.unpack $ kt_port conn)


-------------------
-- TSV-RPC Requests
-------------------

-- | The result of a Kyoto Tycoon operation.
type KTResult a = Either (ReturnStatus, ByteString) a

-- | Perform a TSV-RPC request. The arguments are the command name and the (key,
-- value) pair list to send over. Returns either a 'ReturnStatus' and an error
-- message, or a list of (key, value) pairs from the server.
tsvrpc :: KyotoServer -> ByteString -> [(ByteString, ByteString)] 
       -> IO (KTResult [(ByteString, ByteString)])
tsvrpc conn command args = do
  let request = makeRequest (kt_host conn) (kt_port conn) command args
      sock    = kt_socket conn
  toByteStringIOWith 1024 (sendAll sock) request
  parsed <- parseWith (recv sock 1024) parseTSVReply ""
  case parsed of
    Fail _ _ err -> return $ Left (StatusUnknown, B.pack err)
    Done _ r     -> return r
    Partial _    -> return $ Left (StatusUnknown, "Incomplete response from server")

-- Transform the 'Right' argument of the return value from 'tsvrpc'
processGood :: ([(ByteString, ByteString)] -> a)
            -> KTResult [(ByteString, ByteString)] -> KTResult a
processGood f (Right x) = Right (f x)
processGood _ (Left x)  = Left x

-- Helper function for doing optional arguments.
maybeArg :: [(ByteString, ByteString)] -> (ByteString, Maybe ByteString)
         -> [(ByteString, ByteString)]
lst `maybeArg` (name, Just value) = (name, value) : lst
lst `maybeArg` (_,    Nothing)    = lst
{-# INLINE maybeArg #-}

-- Helper function for doing several optional arguments.
maybeArgs :: [(ByteString, ByteString)] -> [(ByteString, Maybe ByteString)]
          -> [(ByteString, ByteString)]
lst `maybeArgs` args = foldl' maybeArg lst args 
{-# INLINE maybeArgs #-}

-- | @get conn db key@ gets @key@ from the optional @db@. Returns either an
-- error or a 'ByteString'.
get :: KyotoServer -> Maybe ByteString -> ByteString -> IO (KTResult ByteString)
get conn db key = do
  resp <- tsvrpc conn "get" $ [("key", key)] `maybeArg` ("DB", db)
  return $ processGood (snd . head) resp

-- | @set conn db key value ttl@ sets @key@ to @value@ in database @db@
-- (optional) with expiration time @ttl@ (optional, in seconds).
set :: KyotoServer -> Maybe ByteString -> ByteString -> ByteString 
    -> Maybe Int -> IO (KTResult ())
set conn db key value ttl = do
  let args = [("key", key), ("value", value)] `maybeArgs` [("DB", db), ("xt", ttlBS)]
      ttlBS = B.pack . show <$> ttl
  resp <- tsvrpc conn "set" args
  return $ processGood (const ()) resp

remove :: KyotoServer -> Maybe ByteString -> ByteString -> IO (KTResult ())
remove conn db key = do
  resp <- tsvrpc conn "remove" $ [("key", key)] `maybeArg` ("DB", db)
  return $ processGood (const ()) resp

-----------------
-- Debugging code
-----------------

foo :: IO ()
foo = do
  conn <- connect "127.0.0.1" "1978"
  print conn
  let key = "09012345678"
  _ <- remove conn Nothing key
  _ <- set conn Nothing key "John Doe" (Just 12)
  Right val <- get conn Nothing key
  B.putStrLn val
  disconnect conn
