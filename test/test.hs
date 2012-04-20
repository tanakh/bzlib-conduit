{-# LANGUAGE ViewPatterns #-}
import Control.Applicative
import Control.Monad
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Conduit
import Data.Conduit.Binary as B
import Data.Conduit.BZlib
import Data.Conduit.List as C
import System.Random

import Test.Hspec.Monadic
import Test.Hspec.HUnit
import Test.Hspec.QuickCheck
import Test.HUnit
import Test.QuickCheck
import Test.QuickCheck.Property

import Prelude as P

main :: IO ()
main = hspecX $ do
  describe "decompress" $ do
    forM_ ["sample1", "sample2", "sample3"] $ \file -> do
      it ("correctly " ++ file ++ ".bz2") $ do
        dec <- runResourceT $ sourceFile ("test/" ++ file ++ ".bz2") =$= bunzip2 $$ B.take (10^9)
        ref <- L.readFile ("test/" ++ file ++ ".ref")
        assert $ dec == ref

  describe "compress" $ do
    prop ". decompress == id" $ \((`mod` (2^16)) . abs -> n) -> morallyDubiousIOProperty $ do
      let bsize = 8192
      ss <- P.takeWhile (not. null) . P.map (P.take bsize) . iterate (P.drop bsize) <$> replicateM (abs n) randomIO
      dest <- runResourceT $ C.sourceList (P.map S.pack ss) =$= bzip2 =$= bunzip2 $$ B.take (10^9)
      return $ dest == L.pack (concat ss)
