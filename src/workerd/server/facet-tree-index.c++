#include "facet-tree-index.h"

#include <kj/debug.h>
#include <kj/io.h>

namespace workerd::server {

using kj::byte;
using kj::uint;

namespace {

class EraseRegulator final: public SqliteDatabase::Regulator {
 public:
  bool isAllowedTrigger(kj::StringPtr name) const override {
    return name == "removed";
  }
};
constexpr EraseRegulator ERASE_REGULATOR;

class SqliteFacetIndex final: public FacetIndex {
 public:
  SqliteFacetIndex(
      kj::Own<SqliteDatabase> db, kj::Function<kj::Promise<void>(SqliteDatabase&)> confirmCommit)
      : db(kj::mv(db)),
        confirmCommit(kj::mv(confirmCommit)) {
    this->db->run(R"(
      CREATE TABLE IF NOT EXISTS facets (
        id INTEGER PRIMARY KEY CHECK(id BETWEEN 1 AND 65535),
        parent INTEGER NOT NULL CHECK(parent >= 0 AND parent < id),
        name TEXT NOT NULL CHECK(length(CAST(name AS BLOB)) BETWEEN 1 AND 65535),
        UNIQUE(parent, name)
      );
      CREATE TABLE IF NOT EXISTS facet_sequence (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        last_id INTEGER NOT NULL
      );
      INSERT OR IGNORE INTO facet_sequence SELECT 1, COALESCE(MAX(id), 0) FROM facets;
    )");
  }

  uint getId(uint parent, kj::StringPtr name) override {
    KJ_REQUIRE(name.size() > 0 && name.size() <= MAX_ID, "Invalid facet name length");
    {
      auto query = db->run("SELECT id FROM facets WHERE parent = ? AND name = ?", parent, name);
      if (!query.isDone()) return query.getInt64(0);
    }
    uint nextId;
    {
      auto query = db->run("SELECT last_id + 1 FROM facet_sequence WHERE singleton = 1");
      nextId = query.getInt64(0);
    }
    KJ_REQUIRE(nextId <= MAX_ID, "Maximum number of facets exceeded");
    KJ_REQUIRE(parent < nextId, "Invalid parent ID");
    if (parent != 0) {
      auto query = db->run("SELECT id FROM facets WHERE id = ?", parent);
      KJ_REQUIRE(!query.isDone(), "Invalid parent ID");
    }
    // Persist the allocation before inserting the name. A failed insert can skip an ID.
    db->run("UPDATE facet_sequence SET last_id = ? WHERE singleton = 1", nextId);
    db->run("INSERT INTO facets VALUES (?, ?, ?)", nextId, parent, name);
    return nextId;
  }

  void forEachChild(uint parent, kj::FunctionParam<void(uint, kj::StringPtr)> callback) override {
    auto query = db->run("SELECT id, name FROM facets WHERE parent = ? ORDER BY name", parent);
    while (!query.isDone()) {
      callback(query.getInt64(0), query.getText(1));
      query.nextRow();
    }
  }

  kj::Promise<void> confirm() override {
    return confirmCommit(*db);
  }

  bool eraseFacet(uint parent, kj::StringPtr name) override {
    db->run({.regulator = ERASE_REGULATOR}, R"(
      WITH RECURSIVE removed(id) AS (
        SELECT id FROM facets WHERE parent = ? AND name = ?
        UNION ALL SELECT facets.id FROM facets JOIN removed ON facets.parent = removed.id
      ) DELETE FROM facets WHERE id IN (SELECT id FROM removed)
    )",
        parent, name);
    return true;
  }

  bool eraseDescendants(uint parent) override {
    db->run({.regulator = ERASE_REGULATOR}, R"(
      WITH RECURSIVE removed(id) AS (
        SELECT id FROM facets WHERE parent = ?
        UNION ALL SELECT facets.id FROM facets JOIN removed ON facets.parent = removed.id
      ) DELETE FROM facets WHERE id IN (SELECT id FROM removed)
    )",
        parent);
    return true;
  }

 private:
  static constexpr uint MAX_ID = 65535;
  kj::Own<SqliteDatabase> db;
  kj::Function<kj::Promise<void>(SqliteDatabase&)> confirmCommit;
};

}  // namespace

kj::Own<FacetIndex> newSqliteFacetIndex(
    kj::Own<SqliteDatabase> db, kj::Function<kj::Promise<void>(SqliteDatabase&)> confirmCommit) {
  return kj::heap<SqliteFacetIndex>(kj::mv(db), kj::mv(confirmCommit));
}

FacetTreeIndex::FacetTreeIndex(kj::Own<const kj::File> fileParam): file(kj::mv(fileParam)) {
  // Read the file to populate the initial index

  auto fileBytes = file->readAllBytes();

  // Check if the magic number is present.
  //
  // If the file size is less than or equal to the magic number size itself, it's possible that a
  // previous session suffered a failure while writing the magic number. In that case we can assume
  // nothing was ever written to the index, so we just rewrite it and start over.
  if (fileBytes.size() <= sizeof(MAGIC_NUMBER)) {
    // New file, initialize with magic number.
    file->write(0, kj::asBytes(MAGIC_NUMBER));
    file->datasync();
    offset = sizeof(MAGIC_NUMBER);
    return;
  }

  // On the other hand, because we datasync() immediately after writing the magic number, we can
  // assume that if _more_ bytes are written than just the magic number, then a failure did _not_
  // occurr during the writing of the magic number, and therefore, if it contains the wrong bytes,
  // the file must be in a format we don't recognize.
  uint64_t magic = 0;
  memcpy(&magic, fileBytes.begin(), sizeof(magic));
  KJ_REQUIRE(magic == MAGIC_NUMBER, "unknown magic number on facet tree index");
  offset = sizeof(magic);

  // Read entries
  while (offset + sizeof(EntryHeader) <= fileBytes.size()) {
    KJ_REQUIRE(nextId() <= MAX_ID, "Maximum number of facets exceeded");

    EntryHeader header;
    memcpy(&header, fileBytes.begin() + offset, sizeof(header));

    // Validation checks
    if (header.nameLength == 0) {
      // Empty name is invalid.
      break;
    }

    if (offset + sizeof(EntryHeader) + header.nameLength > fileBytes.size()) {
      // Name extends beyond file bounds, invalid.
      break;
    }

    if (header.parentId >= nextId()) {
      // Invalid parent ID (parent must already exist).
      break;
    }

    // Extract the name
    kj::String name = kj::heapString(
        reinterpret_cast<const char*>(fileBytes.begin() + offset + sizeof(EntryHeader)),
        header.nameLength);

    bool duplicate = false;
    entries.upsert(
        Entry{header.parentId, kj::mv(name)}, [&](Entry&, Entry&&) { duplicate = true; });

    if (duplicate) {
      // Duplicate entry is invalid.
      break;
    }

    // Entry was valid and processed successfully, now we can update the offset
    offset += sizeof(EntryHeader) + header.nameLength;
  }

  if (offset < fileBytes.size()) {
    // It appears we stopped at a corrupted entry. We assume such corruption can only be the result
    // of a power failure in the middle of writing an entry during a past session. Any entry which
    // was written but not synced can be presumed to have never been used, so we can simply
    // truncate it from the file.
    file->truncate(offset);
  }
}

uint FacetTreeIndex::getId(uint parent, kj::StringPtr name) {
  KJ_REQUIRE(name.size() > 0, "Facet name cannot be empty");
  KJ_REQUIRE(name.size() <= (uint16_t)kj::maxValue, "Facet name too long");
  KJ_REQUIRE(parent <= entries.size(), "Invalid parent ID");

  // Use findOrCreate to either find an existing entry or create a new one
  auto& entry = entries.findOrCreate(EntryPtr{parent, name}, [&]() -> Entry {
    // New entry, need to assign a new ID and append to file
    KJ_REQUIRE(nextId() <= MAX_ID, "Maximum number of facets exceeded");

    // Prepare entry data
    EntryHeader header{
      .parentId = static_cast<uint16_t>(parent),
      .nameLength = static_cast<uint16_t>(name.size()),
    };

// Don't whine about VLA being non-standard.
#pragma clang diagnostic ignored "-Wvla-cxx-extension"

    size_t entrySize = sizeof(EntryHeader) + header.nameLength;
    byte entryData[entrySize];
    memcpy(entryData, &header, sizeof(header));
    memcpy(entryData + sizeof(EntryHeader), name.begin(), header.nameLength);

    file->write(offset, kj::arrayPtr(entryData, entrySize));

    // We don't want to return an entry that might disappear after a power failure, so sync it
    // now.
    file->datasync();

    offset += entrySize;

    return Entry{parent, kj::heapString(name)};
  });

  // Calculate the ID based on the entry's position in the set
  // Root facet (ID 0) isn't in the entries set, so add 1 to the index
  return 1 + (&entry - entries.begin());
}

}  // namespace workerd::server
