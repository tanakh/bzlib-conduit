{-# LANGUAGE RecordWildCards #-}
module Data.Conduit.BZlib (
  compress,
  decompress,

  bzip2,
  bunzip2,

  CompressParams(..),
  DecompressParams(..),
  def,
  ) where

import Control.Monad as CM
import Control.Monad.Trans
import Control.Monad.Trans.Resource
import qualified Data.ByteString as S
import qualified Data.ByteString.Unsafe as S
import Data.Conduit
import Data.Default.Class
import Data.Maybe
import Data.IORef
import Foreign
import Foreign.C

import Data.Conduit.BZlib.Internal

-- | Compression parameters
data CompressParams
  = CompressParams
    { cpBlockSize  :: Int -- ^ Compress level [1..9]. default is 9.
    , cpVerbosity  :: Int -- ^ Verbosity mode [0..4]. default is 0.
    , cpWorkFactor :: Int -- ^ Work factor [0..250]. default is 30.
    }

instance Default CompressParams where
  def = CompressParams 9 0 30

-- | Decompression parameters
data DecompressParams
  = DecompressParams
    { dpVerbosity :: Int -- ^ Verbosity mode [0..4]. default is 0
    , dpSmall     :: Bool -- ^ If True, use an algorithm uses less memory but slow. default is False
    }

instance Default DecompressParams where
  def = DecompressParams 0 False

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

fillInput :: Ptr C'bz_stream -> IORef (Ptr CChar, Int) -> S.ByteString -> IO ()
fillInput ptr mv bs = S.unsafeUseAsCStringLen bs $ \(p, len) -> do
  (buf, bsize) <- readIORef mv
  let nsize = head [ s | x <- [0..], let s = bsize * 2 ^ (x :: Int), s >= len ]
  nbuf <- if nsize >= bsize then reallocBytes buf nsize else return buf
  copyBytes nbuf p len
  poke (p'bz_stream'avail_in ptr) $ fromIntegral len
  poke (p'bz_stream'next_in ptr) nbuf
  writeIORef mv (nbuf, nsize)

throwIfMinus :: String -> IO CInt -> IO CInt
throwIfMinus s m = do
  r <- m
  when (r < 0) $ throwM $ userError $ s ++ ": " ++ show r
  return r

throwIfMinus_ :: String -> IO CInt -> IO ()
throwIfMinus_ s m = CM.void $ throwIfMinus s m

allocateStream :: MonadResource m => m (Ptr C'bz_stream, IORef (Ptr CChar, Int))
allocateStream = do
  (_, ptr)    <- allocate malloc free
  (_, inbuf)  <- allocate (mallocBytes bufSize >>= \p -> newIORef (p, bufSize))
                          (\mv -> readIORef mv >>= \(p, _) -> free p)
  (_, outbuf) <- allocate (mallocBytes bufSize) free
  liftIO $ poke ptr $ C'bz_stream
    { c'bz_stream'next_in        = nullPtr
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
  return (ptr, inbuf)

-- | Compress a stream of ByteStrings.
compress
  :: MonadResource m
     => CompressParams -- ^ Compress parameter
     -> ConduitT S.ByteString S.ByteString m ()
compress CompressParams {..} = do
  (ptr, inbuf) <- lift $ allocateStream
  _ <- lift $ allocate
    (throwIfMinus_ "bzCompressInit" $
     c'BZ2_bzCompressInit ptr
     (fromIntegral cpBlockSize)
     (fromIntegral cpVerbosity)
     (fromIntegral cpWorkFactor))
    (\_ -> throwIfMinus_ "bzCompressEnd" $ c'BZ2_bzCompressEnd ptr)

  let loop = do
        mbinp <- await
        case mbinp of
          Just inp -> do
            when (not $ S.null inp) $ do
              liftIO $ fillInput ptr inbuf inp
              yields ptr c'BZ_RUN
            loop
          Nothing -> do
            yields ptr c'BZ_FINISH
  loop
  where
    yields ptr action = do
      cont <- liftIO $ throwIfMinus "bzCompress" $ c'BZ2_bzCompress ptr action
      mbout <- liftIO $ getAvailOut ptr
      when (isJust mbout) $
        yield $ fromJust mbout
      availIn <- liftIO $ peek $ p'bz_stream'avail_in ptr
      when (availIn > 0 || action == c'BZ_FINISH && cont /= c'BZ_STREAM_END) $
        yields ptr action

-- | Decompress a stream of ByteStrings.
decompress
  :: MonadResource m
     => DecompressParams -- ^ Decompress parameter
     -> ConduitT S.ByteString S.ByteString m ()
decompress DecompressParams {..} = do
  (ptr, inbuf) <- lift $ allocateStream
  _ <- lift $ allocate
    (throwIfMinus_ "bzDecompressInit" $
     c'BZ2_bzDecompressInit ptr (fromIntegral dpVerbosity) (fromBool dpSmall))
    (\_ -> throwIfMinus_ "bzDecompressEnd" $ c'BZ2_bzDecompressEnd ptr)

  let loop = do
        mbinp <- await
        case mbinp of
          Just inp | not (S.null inp) -> do
            liftIO $ fillInput ptr inbuf inp
            cont <- yields ptr
            when cont $ loop
          Just _ -> do
            loop
          Nothing -> do
            liftIO $ throwM $ userError "unexpected EOF on decompress"
  loop
  where
    yields ptr = do
      ret <- liftIO $ throwIfMinus "BZ2_bzDecompress" $ c'BZ2_bzDecompress ptr
      mbout <- liftIO $ getAvailOut ptr
      when (isJust mbout) $
        yield $ fromJust mbout
      availIn <- liftIO $ peek $ p'bz_stream'avail_in ptr
      if availIn > 0
        then yields ptr
        else return $ ret == c'BZ_OK

-- | bzip2 compression with default parameters.
bzip2 :: MonadResource m => ConduitT S.ByteString S.ByteString m ()
bzip2 = compress def

-- | bzip2 decompression with default parameters.
bunzip2 :: MonadResource m => ConduitT S.ByteString S.ByteString m ()
bunzip2 = decompress def
