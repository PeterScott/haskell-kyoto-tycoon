{-# LANGUAGE OverloadedStrings, BangPatterns #-}
module Database.KyotoTycoon where

import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.ByteString.Char8 (ByteString)
import Data.Attoparsec
import qualified Data.Attoparsec.Char8 as P8
import Blaze.ByteString.Builder
import Blaze.ByteString.Builder.ByteString
import Blaze.ByteString.Builder.Char8 (fromString, fromChar)
import Control.Applicative hiding (many)
import Data.Monoid
import Data.List (foldl')
import Database.KyotoTycoon.TSVRPC

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

-- Building TSV-RPC requests




foo = makeRequest "localhost" "1978" "set" [("DB", "phone_db.kch"), ("key", "09012345678"), ("value", "John Doe")]
