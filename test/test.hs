{-# LANGUAGE ViewPatterns #-}
import Control.Applicative
import Control.Monad
import Control.Monad.Trans.Resource (runResourceT)
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Conduit
import Data.Conduit.Binary as B
import Data.Conduit.BZlib
import Data.Conduit.List as C
import System.Random

import Test.Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Test.QuickCheck.Property

import Prelude as P

main :: IO ()
main = hspec $ do
  describe "decompress" $ do
    forM_ ["sample1", "sample2", "sample3"] $ \file -> do
      it ("correctly " ++ file ++ ".bz2") $ do
        dec <- runResourceT $ do
          sourceFile ("test/" ++ file ++ ".bz2") =$= bunzip2 $$ B.take (10^9)
        ref <- L.readFile ("test/" ++ file ++ ".ref")
        dec `shouldBe` ref

  describe "compress" $ do
    prop ". decompress == id" $ \((`mod` (2^16)) . abs -> n) -> do
      morallyDubiousIOProperty $ do
        let bsize = 8192
        ss <- P.takeWhile (not. null)
              . P.map (P.take bsize)
              . P.iterate (P.drop bsize)
              <$> replicateM (abs n) randomIO
        dest <- runResourceT $ do
          C.sourceList (P.map S.pack ss) =$= bzip2 =$= bunzip2 $$ B.take (10^9)
        return $ dest == L.pack (P.concat ss)
