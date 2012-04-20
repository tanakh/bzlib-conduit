module Data.Conduit.BZlib (
  compress,
  decompress,
  bzip2,
  bunzip2,
  
  CompressParams(..),
  DecompressParams(..),
  def,
  ) where

import Control.Applicative
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Resource
import qualified Data.ByteString as S
import Data.Conduit
import Data.Default
import Data.Maybe
import Foreign
import Foreign.C

import Data.Conduit.BZlib.Internal

data CompressParams = CompressParams
instance Default CompressParams where
data DecompressParams = DecompressParams
instance Default DecompressParams where

-- | Compress (deflate) a stream of ByteStrings.
compress
  :: MonadResource m
     => CompressParams -- ^ Compress parameter
     -> Conduit S.ByteString m S.ByteString
compress param = do
  undefined
{-
  (_, bzs) <- lift $ allocate (c'BZ2_bzCompressInit 1 1 1) c'BZ2_bzComp
  running bzs
  where
    running bzs = do
      mb <- await
      case mb of
        Just dat -> do
          liftIO $ do
            setInput bzs dat
            compressFlush bzs
          flushing bzs
        Nothing -> do
          compressFinish bzs
          finishing bzs
    
    flushing bzs = do
      out <- liftIO $ getOutput bzs
      yield out
      cont <- compressFlush bzs
      if cont
        then flushing bzs
        else running bzs
    
    finishing bzs = do
      out <- liftIO $ getOutput bzs
      yield out
      cont <- compressFinish bzs
      when cont $ finishing bzs
-}

bufSize :: Int
bufSize = 4096

getAvailOut :: Ptr C'bz_stream -> IO (Maybe S.ByteString)
getAvailOut ptr = do
  availOut <- liftIO $ fromIntegral <$> (peek $ p'bz_stream'avail_out ptr)
  if availOut < bufSize
    then do
      let len = bufSize - availOut
      p <- (`plusPtr` (-len)) <$> (peek $ p'bz_stream'next_out ptr)
      out <- S.packCStringLen (p, fromIntegral len)
      poke (p'bz_stream'next_out ptr) p
      poke (p'bz_stream'avail_out ptr) (fromIntegral bufSize)
      return $ Just out
    else do
    return Nothing

fillInput :: Ptr C'bz_stream -> S.ByteString -> IO ()
fillInput ptr bs = S.useAsCStringLen bs $ \(p, len) -> do
  org <- (`plusPtr` (-bufSize)) <$> (peek $ p'bz_stream'next_in ptr)
  let q = org `plusPtr` (bufSize - len)
  copyBytes q p len
  poke (p'bz_stream'avail_in ptr) $ fromIntegral len
  poke (p'bz_stream'next_in ptr) q

throwIfMinus :: String -> IO CInt -> IO CInt
throwIfMinus s m = do
  r <- m
  when (r < 0) $ monadThrow $ userError $ s ++ ": " ++ show r
  return r

-- | Decompress (inflate) a stream of ByteStrings.
decompress
  :: MonadResource m
     => DecompressParams -- ^ Decompress parameter
     -> Conduit S.ByteString m S.ByteString
decompress param = do
  (_, ptr)    <- lift $ allocate malloc free
  (_, inpbuf) <- lift $ allocate (mallocBytes bufSize) free
  (_, outbuf) <- lift $ allocate (mallocBytes bufSize) free
  liftIO $ poke ptr $ C'bz_stream
    { c'bz_stream'next_in        = inpbuf `plusPtr` bufSize
    , c'bz_stream'avail_in       = 0
    , c'bz_stream'total_in_lo32  = 0
    , c'bz_stream'total_in_hi32  = 0
    , c'bz_stream'next_out       = outbuf
    , c'bz_stream'avail_out      = fromIntegral bufSize
    , c'bz_stream'total_out_lo32 = 0
    , c'bz_stream'total_out_hi32 = 0
    , c'bz_stream'state          = nullPtr
    , c'bz_stream'bzalloc        = nullPtr
    , c'bz_stream'bzfree         = nullPtr
    , c'bz_stream'opaque         = nullPtr
    }
  _ <- lift $ allocate
    (throwErrnoIf_ (/= c'BZ_OK) "bzDecompressInit" $ c'BZ2_bzDecompressInit ptr 1 0)
    (\_ -> throwErrnoIf_ (/= c'BZ_OK) "bzDecompressEnd" $ c'BZ2_bzDecompressEnd ptr)
  go ptr
  where
    go ptr = do
      mbinp <- await
      case mbinp of
        Nothing -> do
          lift $ monadThrow $ userError "unexpected EOF on decompress"
        Just inp -> do
          liftIO $ fillInput ptr inp
          cont <- yields ptr
          when cont $ go ptr
    
    decomp ptr = liftIO $ do
      ret <- throwIfMinus "BZ2_bzDecompress" $ c'BZ2_bzDecompress ptr
      return $ ret == c'BZ_OK

    yields ptr = do
      cont <- decomp ptr
      mbo <- liftIO $ getAvailOut ptr
      when (isJust mbo) $
        yield $ fromJust mbo
      availIn <- liftIO $ fromIntegral <$> (peek $ p'bz_stream'avail_in ptr)
      if availIn > 0
        then yields ptr
        else return cont

-- | bzip2 compression with default parameters.
bzip2 :: MonadResource m => Conduit S.ByteString m S.ByteString
bzip2 = compress def

-- | bzip2 decompression with default parameters.
bunzip2 :: MonadResource m => Conduit S.ByteString m S.ByteString
bunzip2 = decompress def
