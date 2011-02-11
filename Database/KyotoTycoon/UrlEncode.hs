{-# LANGUAGE OverloadedStrings, BangPatterns #-}
module Database.KyotoTycoon.UrlEncode (quote, unquote, quoteLen) where

import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.ByteString.Internal (unsafeCreate, w2c)
import Data.ByteString.Unsafe
import Foreign.Storable
import Foreign.Ptr
import Data.Char
import Data.Word


multiSub :: Int -> (Word8 -> [Word8]) -> ByteString -> ByteString
multiSub len replace_word s = unsafeCreate len $ \p -> go p 0 (p `plusPtr` len)
    where go !p !i !end | p == end  = return ()
                        | otherwise = doChunk p i end (replace_word $ unsafeIndex s i)
          doChunk !p !i !end []     = go p (i+1) end
          doChunk !p !i !end (x:xs) = poke p x >> doChunk (p `plusPtr` 1) i end xs
{-# INLINE multiSub #-}

toHex :: Word8 -> Word8
toHex n | n < 10    = 48 + n
        | otherwise = 55 + n
{-# INLINE toHex #-}

fromHex :: Word8 -> Word8
fromHex n | n >= 65   = n - 55
          | otherwise = n - 48
{-# INLINE fromHex #-}

readHex2 :: ByteString -> Word8
readHex2 s = d1*16 + d2
    where d1 = fromHex $ unsafeIndex s 0
          d2 = fromHex $ unsafeIndex s 1
{-# INLINE readHex2 #-}

is_safe :: Char -> Bool
is_safe c = isAlphaNum c || B.elem c "_.-/"
{-# INLINE is_safe #-}

-- | Replace special characters in string using the %xx escape codes.
quote :: ByteString -> ByteString
quote s = multiSub (quoteLen s) replace_word s
    where replace_word w = if is_safe (w2c w) then replace_safe else replace_unsafe
              where replace_safe   = [w]
                    replace_unsafe = [37, toHex $ w `div` 16, toHex $ w `mod` 16]

-- | The inverse of 'quote'
unquote :: ByteString -> ByteString
unquote str = B.pack (go str)
    where go s = case B.uncons s of
                   Just ('%', tl) -> (w2c $ readHex2 tl) : go (unsafeDrop 3 s)
                   Just (c,   tl) -> c : go tl
                   Nothing        -> []

-- | Length of a quoted string
quoteLen :: ByteString -> Int
quoteLen s = B.foldl' (\n -> \c -> n + (char_len c)) 0 s
    where char_len c = if is_safe c then 1 else 3
