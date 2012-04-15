import Control.Monad
import qualified Data.ByteString.Lazy as L
import Data.Conduit
import Data.Conduit.Binary as B
import Data.Conduit.BZlib
import Test.Hspec.Monadic
import Test.Hspec.HUnit
import Test.HUnit

main :: IO ()
main = hspecX $ do
  describe "decoder" $ do
    forM_ ["sample1", "sample2", "sample3"] $ \file -> do
      it ("decodes " ++ file ++ ".bz2") $ do
        dec <- runResourceT $ sourceFile ("test/" ++ file ++ ".bz2") =$= bunzip2 $$ B.take (10^9)
        ref <- L.readFile ("test/" ++ file ++ ".ref")
        assert $ dec == ref
