import Data.Conduit
import Data.Conduit.Binary
import Data.Conduit.List as C
import Data.Conduit.BZlib
import System.Environment

main :: IO ()
main = do
  [file] <- getArgs
  runResourceT $ sourceFile file =$= bunzip2 $$ sinkNull
