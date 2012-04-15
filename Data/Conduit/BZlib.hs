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
import Foreign
import Foreign.C
import System.IO
import System.IO.Error

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

-- | Decompress (inflate) a stream of ByteStrings.
decompress
  :: MonadResource m
     => DecompressParams -- ^ Decompress parameter
     -> Conduit S.ByteString m S.ByteString
decompress param = do
  (_, ptr) <- lift $ allocate malloc free
  (_, outbuf) <- lift $ allocate (mallocBytes bufSize) free
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
  _ <- lift $ allocate
    (throwErrnoIf_ (/= c'BZ_OK) "bzDecompressInit" $ c'BZ2_bzDecompressInit ptr 1 0)
    (\_ -> throwErrnoIf_ (/= c'BZ_OK) "bzDecompressEnd" $ c'BZ2_bzDecompressEnd ptr)
  go ptr
  mbo <- liftIO $ getAvailOut ptr
  case mbo of
    Just out -> yield out
    Nothing -> return ()
  where
    go ptr = do
      mbo <- liftIO $ getAvailOut ptr
      case mbo of
        Just out -> do
          yield out
          cont <- liftIO $ decomp ptr
          when cont $ go ptr
        Nothing -> do
          availIn <- liftIO $ peek $ p'bz_stream'avail_in ptr
          if availIn > 0
            then do
            cont <- liftIO $ decomp ptr
            -- liftIO $ hPrint stderr =<< peek ptr
            when cont $ go ptr
            else do
            mbinp <- await
            case mbinp of
              Just inp -> do
                cont <- liftIO $ S.useAsCStringLen inp $ \(p, len) -> do
                  poke (p'bz_stream'next_in ptr) p
                  poke (p'bz_stream'avail_in ptr) $ fromIntegral len
                  -- hPrint stderr =<< peek ptr
                  r <- decomp ptr
                  -- hPrint stderr =<< peek ptr
                  return r
                when cont $ go ptr
              Nothing -> do
                lift $ monadThrow $ userError "invalid eof of input"

    decomp ptr = liftIO $ do
      ret <- throwErrnoIf (< 0) "BZ2_bzDecompress" $ c'BZ2_bzDecompress ptr
      return $ ret == c'BZ_OK

-- | bzip2 compression with default parameters.
bzip2 :: MonadResource m => Conduit S.ByteString m S.ByteString
bzip2 = compress def

-- | bzip2 decompression with default parameters.
bunzip2 :: MonadResource m => Conduit S.ByteString m S.ByteString
bunzip2 = decompress def
