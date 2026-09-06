// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/util/sqlite.h>

#include <kj/async-io.h>
#include <kj/filesystem.h>

namespace workerd::server {

// Owns the storage resources for one Durable Object namespace. Implementations
// can store SQLite databases and the small facet index in different systems.
class ActorStorageNamespace {
 public:
  virtual kj::Own<SqliteDatabase> openDatabase(
      kj::Path path, kj::Maybe<kj::WriteMode> mode = kj::none) = 0;
  virtual void configureDatabase(SqliteDatabase& db) = 0;
  virtual kj::Promise<void> confirmDatabaseCommit(SqliteDatabase&, kj::Timer&) {
    return kj::READY_NOW;
  }

  virtual kj::Own<const kj::File> openAuxiliaryFile(
      kj::Path path, kj::WriteMode mode) = 0;
  virtual kj::Maybe<kj::Own<const kj::File>> tryOpenAuxiliaryFile(
      kj::Path path, kj::WriteMode mode) = 0;

  // A database can include journal files that are private to the backend.
  virtual void removeDatabase(kj::PathPtr path) = 0;
  virtual bool cloneDatabase(kj::PathPtr source, kj::PathPtr destination) = 0;

  virtual ~ActorStorageNamespace() noexcept(false) = default;
};

// Creates isolated namespace storage for a worker's Durable Object classes.
class ActorStorageBackend {
 public:
  virtual kj::Own<ActorStorageNamespace> openNamespace(kj::StringPtr uniqueKey) = 0;
  virtual ~ActorStorageBackend() noexcept(false) = default;
};

// Preserves workerd's existing filesystem-backed behavior.
kj::Own<ActorStorageBackend> newLocalActorStorageBackend(const kj::Directory& root);

struct RemoteLtxActorStorageOptions {
  kj::String extensionPath;
  kj::String replicaUrl;
  kj::String vfsName;
  kj::String syncInterval;
  kj::String cacheDirectory;
  uint64_t pageCacheBytes;
};

kj::Own<ActorStorageBackend> newRemoteLtxActorStorageBackend(
    const kj::Directory& cacheRoot, RemoteLtxActorStorageOptions options);

}  // namespace workerd::server
