{-# LANGUAGE OverloadedStrings, BangPatterns #-}
module Database.KyotoTycoon.TSVRPC (makeRequest, parseTSVReply, ReturnStatus(..)) where

-- We need ByteStrings a lot here
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.ByteString.Char8 (ByteString)
-- The encodings supported by KT
import qualified Data.ByteString.Base64 as Base64
import qualified Database.KyotoTycoon.UrlEncode as UrlEncode
-- All ByteString aggregation happens with Builders
import Blaze.ByteString.Builder
import Blaze.ByteString.Builder.Char8 (fromString, fromChar)
-- Parsing incrementally
import Data.Attoparsec
import qualified Data.Attoparsec.Char8 as P8
-- Miscellaneous imports
import Control.Applicative hiding (many)
import Data.Monoid
import Data.Word
import Data.List (foldl')

------------
-- Encodings
------------

-- | Length of a base64-encoded string
base64Len :: ByteString -> Int
base64Len str = (n + 2 - ((n + 2) `mod` 3)) `div` 3 * 4
    where n = B.length str

-- | Decide whether we should use Base64 or UrlEncode for these arguments, based
-- on which would produce a shorter length.
determineEncoding :: [(ByteString, ByteString)] -> ((ByteString -> ByteString), Char)
determineEncoding args = if b64 < ue then (Base64.encode, 'B') else (UrlEncode.quote, 'U')
    where bothLens (k, v) = (base64Len k + base64Len v, UrlEncode.quoteLen k + UrlEncode.quoteLen v)
          addPair (!x1, !y1) (!x2, !y2) = (x1 + x2, y1 + y2)
          (b64, ue) = foldl' addPair (0, 0) $ map bothLens args


--------------------
-- Building requests
--------------------

postLine :: ByteString -> Builder
postLine command = fromByteString "POST /rpc/" `mappend`
                   fromByteString command      `mappend`
                   fromByteString " HTTP/1.1\r\n"

hostLine :: ByteString -> ByteString -> Builder
hostLine host port = mconcat $ map fromByteString ["Host: ", host, ":", port, "\r\n"]

contentTypeLengthLines :: Char -> Int -> Builder
contentTypeLengthLines enc len = 
    fromByteString "Content-Type: text/tab-separated-values; colenc=" `mappend`
    fromChar enc `mappend` fromByteString "\r\nContent-Length: " `mappend`
    fromString (show len) `mappend` fromByteString "\r\n\r\n"

-- | Make an HTTP request. Arguments are, in order: @host@ and @port@;
-- @command@, the name of the RPC command to execute; @argLines@, a list of
-- @(key, value)@ pairs which are to be encoded in the body of the request. The
-- keys and values should be passed unescaped. This function will perform either
-- URL encoding or Base64 encoding, whichever produces the shorter
-- result. Returns a 'Builder', for the client to use in whatever grand IO
-- scheme there may be.
makeRequest :: ByteString -> ByteString -> ByteString -> [(ByteString, ByteString)] -> Builder
makeRequest host port command argLines = 
    postLine command `mappend` hostLine host port `mappend`
    contentTypeLengthLines encodingChar len `mappend` content
        where content = mconcat $ map (\(k,v) -> fromByteString k `mappend`
                                                 fromChar '\t'    `mappend`
                                                 fromByteString v `mappend`
                                                 fromChar '\n') argsEnc
              len = foldl' (+) 0 $ map (\(k,v) -> (B.length k) + (B.length v) + 2) argsEnc
              argsEnc = map (\(k,v) -> (encode k, encode v)) argLines
              (encode, encodingChar) = determineEncoding argLines


------------------
-- Parsing replies
------------------

-- | The return status of a Kyoto Tyrant request.
data ReturnStatus = StatusSuccess -- ^ the processing is done successfully.
                  | StatusInvalidRequest -- ^ the format of the request is
                                         -- invalid or the arguments are short
                                         -- for the called procedure.
                  | StatusBadResult      -- ^ the processing is done but the
                                         -- result is not fulfill the
                                         -- application logic.
                  | StatusServerError    -- ^ the processing is aborted by fatal
                                         -- error of the server program or the
                                         -- environment.
                  | StatusNotImplemented -- ^ the specified procedure is not
                                         -- implemented.
                  | StatusUnknown        -- ^ Unknown status. Shouldn't happen.
   deriving (Read, Show, Eq)

data Header = EncoderHdr (ByteString -> ByteString)
            | LengthHdr Int
            | OtherHdr
            | NoHdr

toEol :: Parser Word8
toEol = takeTill (==10) *> word8 10

-- Parse a line like "HTTP/1.1 200 OK\r\n"
replyLine :: Parser ReturnStatus
replyLine = toStatus <$> (string "HTTP/1.1 " *> takeWhile1 P8.isDigit_w8 <* toEol)
    where toStatus "200" = StatusSuccess
          toStatus "400" = StatusInvalidRequest
          toStatus "450" = StatusBadResult
          toStatus "500" = StatusServerError
          toStatus "501" = StatusNotImplemented
          toStatus _     = StatusUnknown

-- Parse the Content-Type line, and return the decoding function.
contentTypeLine :: Parser Header
contentTypeLine = string "Type: text/tab-separated-values" *> (noEnc <|> colenc)
    where colenc = string "; colenc=" *> encChar <* P8.endOfLine
          encChar = toEnc <$> satisfy (\w -> w == 85 || w == 66)
          toEnc 85 = EncoderHdr UrlEncode.unquote
          toEnc 66 = EncoderHdr Base64.decodeLenient
          toEnc _  = error "Invalid value encoding from server"
          noEnc = P8.endOfLine >> return (EncoderHdr id)

-- Parse the Content-Length header, and return the content length, in bytes.
contentLengthLine :: Parser Header
contentLengthLine = string "Length: " *> (LengthHdr <$> P8.decimal) <* P8.endOfLine

-- Parse a header starting with "Content-".
contentLine :: Parser Header
contentLine = string "Content-" *> (contentLengthLine <|> contentTypeLine)

noHeader :: Parser Header
noHeader = P8.endOfLine >> return NoHdr

otherHeader :: Parser Header
otherHeader = toEol >> return OtherHdr

-- Parse HTTP headers, not including the response line.
parseHeaders :: Parser (Int, ByteString -> ByteString)
parseHeaders = ph 0 id
    where ph len enc = do
            hdr <- noHeader <|> contentLine <|> otherHeader
            case hdr of
              EncoderHdr e -> ph len e
              LengthHdr  l -> ph l enc
              OtherHdr     -> ph len enc
              NoHdr        -> return (len, enc)

-- hdrs = "HTTP/1.1 200 OK\r\nServer: KyotoTycoon/0.9.33\nDate: Fri, 11 Feb 2011 03:15:30 GMT\nContent-Length: 12\nContent-Type: text/tab-separated-values; colenc=B\n\nNOT HEADERS" :: ByteString
-- testreq = "HTTP/1.1 200 OK\r\nServer: KyotoTycoon/0.9.33\nDate: Fri, 11 Feb 2011 03:15:30 GMT\nContent-Length: 17\nContent-Type: text/tab-separated-values\n\nfoo\tbar\nbaz\tquux\n" :: ByteString
-- testreq2 = "HTTP/1.1 200 OK\r\nServer: KyotoTycoon/0.9.33\nDate: Fri, 11 Feb 2011 03:15:30 GMT\nContent-Length: 24\nContent-Type: text/tab-separated-values; colenc=B\n\naWQ=\tMTIzNDU=\nYWdl\tMzE=\n" :: ByteString
-- testreq3 = "HTTP/1.1 500 Server Error\r\nServer: KyotoTycoon/0.9.33\nDate: Fri, 11 Feb 2011 03:15:30 GMT\nContent-Length: 31\nContent-Type: text/tab-separated-values\n\nERROR\tBad stuff happened, man.\n" :: ByteString

-- Parse a TSV pair, ignoring irrelevant newlines.
parseTSVPair :: Parser (ByteString, ByteString)
parseTSVPair = do
  P8.skipSpace
  key <- takeTill (==9)
  word8 9                       -- TAB
  value <- takeTill P8.isEndOfLine
  P8.endOfLine
  return (key, value)

-- Parse a TSV HTTP reply. After you have received as many bytes as you're going
-- to get, you must feed this parser a blank input, to let it know that it's
-- reached the end. Will not attempt any decoding.
parseTSV :: Parser [(ByteString, ByteString)]
parseTSV = (endOfInput >> return []) <|> iter -- many parseTSVPair -- FIXME
    where iter = do (k, v) <- parseTSVPair
                    others <- parseTSV
                    return $ (k, v) : others

-- | Parse the entire HTTP reply from a TSV-returning operation. Does
-- decoding. Either returns an error message with a return status, or a list of
-- (key, value) ByteString pairs.
parseTSVReply :: Parser (Either (ReturnStatus, ByteString) [(ByteString, ByteString)])
parseTSVReply = do status <- replyLine
                   (len, enc) <- parseHeaders
                   body <- P8.take len
                   case status of
                     StatusSuccess -> goSuccess body enc
                     _             -> return $ Left (status, B.takeWhile (\c -> c /= '\n' && c /= '\r') $ B.drop 6 body)
    where goSuccess body enc = do
            let rawPairs = case feed (parse parseTSV body) "" of
                             Done _ x -> x
                             _        -> fail "Problem parsing TSV from Kyoto Tycoon"
            return $ Right $ map (\(k,v) -> (enc k, enc v)) rawPairs
