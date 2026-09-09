// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "actor-storage.h"

#include <kj/debug.h>

#include <cctype>
#include <cstdlib>

namespace workerd::server {
namespace {

constexpr auto REMOTE_LTX_CONFIRM_POLL_INTERVAL = 10 * kj::MILLISECONDS;
constexpr auto REMOTE_LTX_CONFIRM_TIMEOUT = 5 * kj::SECONDS;

struct DurabilityStatus {
  uint64_t durable;
  uint64_t ticket;
};

kj::Maybe<DurabilityStatus> readDurabilityStatus(SqliteDatabase& db) {
  auto query =
      db.run({.regulator = SqliteDatabase::TRUSTED}, "PRAGMA litestream_durability_status;");
  KJ_REQUIRE(!query.isDone(), "Litestream VFS does not support durability status");
  auto value = query.getText(0);
  if (value == "busy"_kj) return kj::none;
  KJ_REQUIRE(value.size() == 33 && value[16] == ':', "Invalid Litestream durability status", value);
  return DurabilityStatus{kj::str("0x", value.slice(0, 16)).parseAs<uint64_t>(),
    kj::str("0x", value.slice(17)).parseAs<uint64_t>()};
}

class LocalActorStorageNamespace final: public ActorStorageNamespace {
 public:
  explicit LocalActorStorageNamespace(kj::Own<const kj::Directory> directory)
      : directory(kj::mv(directory)),
        vfs(*this->directory) {}

  kj::Own<SqliteDatabase> openDatabase(kj::Path path, kj::Maybe<kj::WriteMode> mode) override {
    auto db = kj::heap<SqliteDatabase>(vfs, kj::mv(path), mode);
    configureDatabase(*db);
    return db;
  }

  void configureDatabase(SqliteDatabase& db) override {
    db.run("PRAGMA journal_mode=WAL;");
  }

  kj::Own<FacetIndex> openFacetIndex(kj::Path path, kj::Timer&) override {
    return kj::heap<FacetTreeIndex>(
        directory->openFile(kj::mv(path), kj::WriteMode::CREATE | kj::WriteMode::MODIFY));
  }

  void removeDatabase(kj::PathPtr path) override {
    directory->tryRemove(path);
    directory->tryRemove(withSuffix(path, "-wal"_kj));
    directory->tryRemove(withSuffix(path, "-shm"_kj));
  }

  bool cloneDatabase(kj::PathPtr source, kj::PathPtr destination) override {
    if (!directory->exists(source)) return false;

    directory->transfer(destination, kj::WriteMode::CREATE, source, kj::TransferMode::COPY);
    cloneIfPresent(withSuffix(source, "-wal"_kj), withSuffix(destination, "-wal"_kj));
    cloneIfPresent(withSuffix(source, "-shm"_kj), withSuffix(destination, "-shm"_kj));
    return true;
  }

 private:
  kj::Own<const kj::Directory> directory;
  SqliteDatabase::Vfs vfs;

  static kj::Path withSuffix(kj::PathPtr path, kj::StringPtr suffix) {
    KJ_REQUIRE(path.size() > 0);
    return path.parent().eval(kj::str(path.basename()[0], suffix));
  }

  void cloneIfPresent(kj::Path source, kj::Path destination) {
    if (directory->exists(source)) {
      directory->transfer(destination, kj::WriteMode::CREATE, source, kj::TransferMode::COPY);
    }
  }
};

class LocalActorStorageBackend final: public ActorStorageBackend {
 public:
  explicit LocalActorStorageBackend(const kj::Directory& root): root(root) {}

  kj::Own<ActorStorageNamespace> openNamespace(kj::StringPtr uniqueKey) override {
    return kj::heap<LocalActorStorageNamespace>(root.openSubdir(
        kj::Path({kj::str(uniqueKey)}), kj::WriteMode::CREATE | kj::WriteMode::MODIFY));
  }

 private:
  const kj::Directory& root;
};

kj::String encodeUriComponent(kj::StringPtr value) {
  static constexpr char HEX[] = "0123456789ABCDEF";
  kj::Vector<char> result(value.size() + 1);
  for (char c: value) {
    auto byte = static_cast<unsigned char>(c);
    if ((byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') ||
        (byte >= '0' && byte <= '9') || c == '-' || c == '_' || c == '.' || c == '~') {
      result.add(c);
    } else {
      result.add('%');
      result.add(HEX[byte >> 4]);
      result.add(HEX[byte & 15]);
    }
  }
  result.add('\0');
  return kj::String(result.releaseAsArray());
}

kj::String appendUrlPath(kj::StringPtr base, kj::StringPtr namespaceKey, kj::StringPtr database) {
  auto separator = base.endsWith("/"_kj) ? ""_kj : "/"_kj;
  return kj::str(
      base, separator, encodeUriComponent(namespaceKey), '/', encodeUriComponent(database));
}

bool enablesHydration(kj::StringPtr value) {
  auto lower = kj::str(value);
  for (auto& c: lower) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return lower == "1"_kj || lower == "true"_kj || lower == "yes"_kj || lower == "on"_kj;
}

class RemoteLtxActorStorageNamespace final: public ActorStorageNamespace {
 public:
  RemoteLtxActorStorageNamespace(kj::Own<const kj::Directory> auxiliaryDirectory,
      kj::String namespaceKey,
      RemoteLtxActorStorageOptions options)
      : auxiliaryDirectory(kj::mv(auxiliaryDirectory)),
        namespaceKey(kj::mv(namespaceKey)),
        options(kj::mv(options)),
        vfs(kj::str(this->options.vfsName),
            [this](kj::PathPtr path) { return makeDatabaseUri(path); }) {}

  kj::Own<SqliteDatabase> openDatabase(kj::Path path, kj::Maybe<kj::WriteMode> mode) override {
    auto db = kj::heap<SqliteDatabase>(vfs, kj::mv(path), mode);
    configureDatabase(*db);
    return db;
  }

  void configureDatabase(SqliteDatabase& db) override {
    auto mode = db.run("PRAGMA journal_mode=DELETE;");
    KJ_REQUIRE(!mode.isDone() && mode.getText(0) == "delete"_kj,
        "remote LTX VFS requires rollback-journal mode");
  }

  kj::Promise<void> confirmDatabaseCommit(SqliteDatabase& db, kj::Timer& timer) override {
    auto deadline = timer.now() + REMOTE_LTX_CONFIRM_TIMEOUT;
    kj::Maybe<uint64_t> ticket;
    for (;;) {
      KJ_IF_SOME(status, readDurabilityStatus(db)) {
        if (ticket == kj::none) ticket = status.ticket;
        if (status.durable >= KJ_ASSERT_NONNULL(ticket)) co_return;
      }
      KJ_REQUIRE(timer.now() < deadline, "Bucket persistence confirmation timed out");
      co_await timer.afterDelay(REMOTE_LTX_CONFIRM_POLL_INTERVAL);
    }
  }

  kj::Own<FacetIndex> openFacetIndex(kj::Path path, kj::Timer& timer) override {
    KJ_REQUIRE(!auxiliaryDirectory->exists(path),
        "Local facet index requires migration before remote storage can open it", path);
    return newSqliteFacetIndex(
        openDatabase(kj::mv(path), kj::WriteMode::CREATE | kj::WriteMode::MODIFY),
        [this, &timer](SqliteDatabase& db) { return confirmDatabaseCommit(db, timer); });
  }

  void removeDatabase(kj::PathPtr path) override {
    vfs.remove(path);
  }

  bool cloneDatabase(kj::PathPtr source, kj::PathPtr destination) override {
    KJ_FAIL_REQUIRE(
        "remote LTX VFS does not support Durable Object facet cloning", source, destination);
  }

 private:
  kj::Own<const kj::Directory> auxiliaryDirectory;
  kj::String namespaceKey;
  RemoteLtxActorStorageOptions options;
  SqliteDatabase::ExternalVfs vfs;

  kj::String makeDatabaseUri(kj::PathPtr path) {
    KJ_REQUIRE(path.size() == 1, "remote LTX database paths must have one component", path);
    kj::StringPtr database = path[0];
    auto logicalName =
        kj::str(encodeUriComponent(namespaceKey), "--", encodeUriComponent(database));
    auto replica = appendUrlPath(options.replicaUrl, namespaceKey, database);
    auto separator = options.cacheDirectory.endsWith("/"_kj) ? ""_kj : "/"_kj;
    auto buffer = kj::str(options.cacheDirectory, separator, encodeUriComponent(namespaceKey), '/',
        encodeUriComponent(database), ".buffer");
    return kj::str("file:", logicalName, "?vfs=", encodeUriComponent(options.vfsName),
        "&replica_url=", encodeUriComponent(replica),
        "&write_enabled=true&hydration_enabled=false&sync_interval=",
        encodeUriComponent(options.syncInterval), "&buffer_path=", encodeUriComponent(buffer),
        "&cache_size=", options.pageCacheBytes);
  }
};

class RemoteLtxActorStorageBackend final: public ActorStorageBackend {
 public:
  RemoteLtxActorStorageBackend(const kj::Directory& cacheRoot, RemoteLtxActorStorageOptions options)
      : cacheRoot(cacheRoot),
        options(kj::mv(options)) {
    if (auto* hydration = getenv("LITESTREAM_HYDRATION_ENABLED")) {
      KJ_REQUIRE(!enablesHydration(hydration),
          "remote LTX storage cannot start when Litestream hydration is enabled");
    }
    SqliteDatabase::ExternalVfs::loadExtension(this->options.extensionPath, this->options.vfsName);
  }

  kj::Own<ActorStorageNamespace> openNamespace(kj::StringPtr uniqueKey) override {
    auto directory = cacheRoot.openSubdir(
        kj::Path({kj::str(uniqueKey)}), kj::WriteMode::CREATE | kj::WriteMode::MODIFY);
    return kj::heap<RemoteLtxActorStorageNamespace>(
        kj::mv(directory), kj::str(uniqueKey), cloneOptions());
  }

 private:
  const kj::Directory& cacheRoot;
  RemoteLtxActorStorageOptions options;

  RemoteLtxActorStorageOptions cloneOptions() {
    return {
      .extensionPath = kj::str(options.extensionPath),
      .replicaUrl = kj::str(options.replicaUrl),
      .vfsName = kj::str(options.vfsName),
      .syncInterval = kj::str(options.syncInterval),
      .cacheDirectory = kj::str(options.cacheDirectory),
      .pageCacheBytes = options.pageCacheBytes,
    };
  }
};

}  // namespace

kj::Own<ActorStorageBackend> newLocalActorStorageBackend(const kj::Directory& root) {
  return kj::heap<LocalActorStorageBackend>(root);
}

kj::Own<ActorStorageBackend> newRemoteLtxActorStorageBackend(
    const kj::Directory& cacheRoot, RemoteLtxActorStorageOptions options) {
  return kj::heap<RemoteLtxActorStorageBackend>(cacheRoot, kj::mv(options));
}

}  // namespace workerd::server
