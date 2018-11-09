{-|
Module    : Database.Orville.Conduit
Copyright : Flipstone Technology Partners 2016-2018
License   : MIT
-}
module Database.Orville.Conduit
  ( selectConduit
  ) where

import Conduit
  ( Acquire
  , ReleaseType(..)
  , allocateAcquire
  , mkAcquire
  , mkAcquireType
  )
import Control.Monad.Catch
import Control.Monad.Trans
import Control.Monad.Trans.Resource (MonadResource, release)
import Data.Conduit
import Data.Pool
import Database.HDBC hiding (withTransaction)

import Database.Orville.Internal.Monad
import Database.Orville.Internal.Select
import Database.Orville.Internal.Types

-- All the masking manual cleanup in this function amounts to a
-- poor man's ResourceT that I *hope* is correct. The constraints
-- hat lead to this are:
--
--   * The immediate purpose of conduit queries to to provide streaming
--     responses in a Happstack App
--
--   * Happstack does not offer a side-effect controlled streaming
--     response solution at the moment. It relies in Lazy Bytestrings
--
--   * The conduit lazy consume specifically warns that you need to
--     ensure you consume the whole list before ResourceT returns,
--     which I cannot guarantee in Happstack (in fact, I believe it
--     specifically will *not* happen that way)
--
--   * Data.Pool.withResource depends on MonadBaseControl, which
--     Conduit does not offer
--
--   * Conduit also does not offer MonadMask, so I cannot use
--     mask/restore in the normal way
--
-- So, we instead we mask exceptions while registering cleanup and
-- finish actions in vars while masked and then ensure those vars
-- are read and executed at the appropriate times.
--
selectConduit ::
     (Monad m, MonadOrville conn m, MonadCatch m, MonadResource m)
  => Select row
  -> ConduitT () row m ()
selectConduit select = do
  pool <- ormEnvPool <$> lift getOrvilleEnv
  (releaseKey, query) <-
    allocateAcquire (acquireStatement pool (selectSql select))
  result <- feedRows (selectBuilder select) query
  -- Note this doesn't use finally to release this, but it will be released
  -- automatically at the end of runResourceT. finally cannot be used here
  -- because Conduit doesn't offer MonadMask. Alternatively we could use
  -- withAllocate here, but that would require an UNLiftIO instance
  release releaseKey
  pure result

acquireConnection :: Pool conn -> Acquire conn
acquireConnection pool =
  fst <$> mkAcquireType (takeResource pool) releaseConnection
  where
    releaseConnection (conn, local) releaseType =
      case releaseType of
        ReleaseEarly -> putResource local conn
        ReleaseNormal -> putResource local conn
        ReleaseException -> destroyResource pool local conn

acquireStatement ::
     IConnection conn => Pool conn -> String -> Acquire Statement
acquireStatement pool sql = do
  conn <- acquireConnection pool
  mkAcquire (prepare conn sql) finish

feedRows ::
     (Monad m, MonadIO m) => FromSql row -> Statement -> ConduitT () row m ()
feedRows builder query = do
  row <- liftIO $ fetchRowAL query
  case runFromSql builder <$> row of
    Nothing -> pure ()
    Just (Left _) -> pure ()
    Just (Right r) -> yield r >> feedRows builder query
