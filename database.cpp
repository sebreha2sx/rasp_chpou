#include "../include/database.h"
#include <iostream>
#include <stdexcept>
#include <ctime>
#include <memory>
#include <vector>
#include <tuple>
//maybe to Redis?
namespace {
    constexpr int ARCHIVE_RETENTION_DAYS = 45;
    constexpr int PIN_EXPIRATION_HOURS = 36;

    struct StatementDeleter {
        void operator()(sqlite3_stmt* stmt) const noexcept {
            if (stmt) sqlite3_finalize(stmt);
        }
    };

    using StatementPtr = std::unique_ptr<sqlite3_stmt, StatementDeleter>;
}

Database::Database(const std::string& path)
    : db_(nullptr), dbPath_(path) {}

Database::~Database() {
    if (db_)
        sqlite3_close(db_);
}

bool Database::initialize() {
    if (sqlite3_open(dbPath_.c_str(), &db_) != SQLITE_OK) {
        std::cerr << "Cannot open database: " << sqlite3_errmsg(db_) << std::endl;
        return false;
    }

    applyPerformanceSettings();

    static const char* CREATE_TABLES_SQL = R"SQL(
        CREATE TABLE IF NOT EXISTS chat_settings (
            chat_id INTEGER PRIMARY KEY,
            group_name TEXT
        );
        
        CREATE TABLE IF NOT EXISTS schedule_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_str TEXT,
            group_name TEXT,
            content TEXT,
            type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS pinned_messages (
            chat_id INTEGER,
            message_id INTEGER,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_schedule_lookup 
            ON schedule_archive (date_str, group_name);
    )SQL";

    if (!execute(CREATE_TABLES_SQL)) {
        std::cerr << "Failed to create database tables" << std::endl;
        return false;
    }

    std::cout << "Database init suc.\n";
    return true;
}

void Database::applyPerformanceSettings() {
    execute("PRAGMA journal_mode=WAL;");
    execute("PRAGMA synchronous=NORMAL;");
    execute("PRAGMA temp_store=MEMORY;");
    execute("PRAGMA cache_size=-10000;");
}

bool Database::execute(const std::string& query) {
    char* errorMsg = nullptr;
    int rc = sqlite3_exec(db_, query.c_str(), nullptr, nullptr, &errorMsg);

    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errorMsg << std::endl;
        sqlite3_free(errorMsg);
        return false;
    }
    return true;
}

StatementPtr Database::prepare(const std::string& query) const {
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, query.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(db_) << std::endl;
        return nullptr;
    }
    return StatementPtr(stmt);
}

void Database::bindText(sqlite3_stmt* stmt, int index, const std::string& value) {
    sqlite3_bind_text(stmt, index, value.c_str(), -1, SQLITE_TRANSIENT);
}

bool Database::executeStatement(sqlite3_stmt* stmt) {
    int rc = sqlite3_step(stmt);
    return rc == SQLITE_DONE || rc == SQLITE_OK;
}

bool Database::saveArchiveEntry(const std::string& date,
                               const std::string& group,
                               const std::string& content,
                               const std::string& type) {
    if (type == "основное") {
        static const char* DELETE_SQL =
            "DELETE FROM schedule_archive WHERE date_str = ? AND group_name = ? AND type = ?";
        auto stmt = prepare(DELETE_SQL);
        if (!stmt) return false;

        bindText(stmt.get(), 1, date);
        bindText(stmt.get(), 2, group);
        bindText(stmt.get(), 3, "основное");
        executeStatement(stmt.get());
    }

    static const char* INSERT_SQL =
        "INSERT INTO schedule_archive (date_str, group_name, content, type) VALUES (?, ?, ?, ?)";
    auto stmt = prepare(INSERT_SQL);
    if (!stmt) return false;

    bindText(stmt.get(), 1, date);
    bindText(stmt.get(), 2, group);
    bindText(stmt.get(), 3, content);
    bindText(stmt.get(), 4, type);

    if (!executeStatement(stmt.get())) {
        std::cerr << "Failed to insert schedule archive record\n";
        return false;
    }

    cleanupOldArchives();
    return true;
}

void Database::cleanupOldArchives() {
    static const char* CLEANUP_SQL = R"SQL(
        DELETE FROM schedule_archive 
        WHERE date_str NOT IN (
            SELECT date_str FROM (
                SELECT date_str FROM schedule_archive 
                GROUP BY date_str 
                ORDER BY MAX(created_at) DESC 
                LIMIT ?
            )
        )
    )SQL";

    auto stmt = prepare(CLEANUP_SQL);
    if (!stmt) return;

    sqlite3_bind_int(stmt.get(), 1, ARCHIVE_RETENTION_DAYS);
    executeStatement(stmt.get());
}

std::vector<Database::ArchiveEntry> Database::getFullArchive() const {
    static const char* QUERY_SQL =
        "SELECT date_str, group_name, content, type FROM schedule_archive ORDER BY created_at ASC";

    std::vector<ArchiveEntry> records;
    auto stmt = prepare(QUERY_SQL);
    if (!stmt) return records;

    while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
        records.emplace_back(
            reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 0)),
            reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 1)),
            reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 2)),
            reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 3))
        );
    }
    return records;
}

bool Database::addPinnedMessage(int64_t chatId, int64_t messageId) {
    static const char* SQL = "INSERT INTO pinned_messages (chat_id, message_id) VALUES (?, ?)";
    auto stmt = prepare(SQL);
    if (!stmt) return false;

    sqlite3_bind_int64(stmt.get(), 1, chatId);
    sqlite3_bind_int64(stmt.get(), 2, messageId);
    return executeStatement(stmt.get());
}

int64_t Database::getLastPinnedMessage(int64_t chatId) const {
    static const char* SQL =
        "SELECT message_id FROM pinned_messages WHERE chat_id = ? "
        "ORDER BY sent_at DESC LIMIT 1";

    auto stmt = prepare(SQL);
    if (!stmt) return 0;

    sqlite3_bind_int64(stmt.get(), 1, chatId);
    if (sqlite3_step(stmt.get()) == SQLITE_ROW)
        return sqlite3_column_int64(stmt.get(), 0);

    return 0;
}

std::vector<std::pair<int64_t, int64_t>> Database::getExpiredPins() {
    static const char* QUERY_SQL =
        "SELECT chat_id, message_id FROM pinned_messages WHERE sent_at <= datetime('now', ?)";
    std::vector<std::pair<int64_t, int64_t>> pins;

    auto stmt = prepare(QUERY_SQL);
    if (!stmt) return pins;

    std::string expiration = "-" + std::to_string(PIN_EXPIRATION_HOURS) + " hours";
    bindText(stmt.get(), 1, expiration);

    while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
        pins.emplace_back(
            sqlite3_column_int64(stmt.get(), 0),
            sqlite3_column_int64(stmt.get(), 1)
        );
    }

    static const char* DELETE_SQL =
        "DELETE FROM pinned_messages WHERE sent_at <= datetime('now', ?)";
    stmt = prepare(DELETE_SQL);
    if (stmt) {
        bindText(stmt.get(), 1, expiration);
        executeStatement(stmt.get());
    }
    return pins;
}

bool Database::saveChatGroup(int64_t chatId, const std::string& groupName) {
    static const char* SQL =
        "INSERT OR REPLACE INTO chat_settings (chat_id, group_name) VALUES (?, ?)";
    auto stmt = prepare(SQL);
    if (!stmt) return false;

    sqlite3_bind_int64(stmt.get(), 1, chatId);
    bindText(stmt.get(), 2, groupName);
    return executeStatement(stmt.get());
}

std::string Database::getChatGroup(int64_t chatId) const {
    static const char* SQL = "SELECT group_name FROM chat_settings WHERE chat_id = ?";
    auto stmt = prepare(SQL);
    if (!stmt) return {};

    sqlite3_bind_int64(stmt.get(), 1, chatId);

    if (sqlite3_step(stmt.get()) == SQLITE_ROW) {
        const unsigned char* text = sqlite3_column_text(stmt.get(), 0);
        return text ? reinterpret_cast<const char*>(text) : "";
    }
    return {};
}

bool Database::deleteChat(int64_t chatId) {
    static const char* SQLS[] = {
        "DELETE FROM chat_settings WHERE chat_id = ?",
        "DELETE FROM pinned_messages WHERE chat_id = ?"
    };

    for (const auto* sql : SQLS) {
        auto stmt = prepare(sql);
        if (!stmt) return false;
        sqlite3_bind_int64(stmt.get(), 1, chatId);
        executeStatement(stmt.get());
    }
    return true;
}

std::vector<std::pair<int64_t, std::string>> Database::getAllChats() const {
    static const char* SQL = "SELECT chat_id, group_name FROM chat_settings";

    std::vector<std::pair<int64_t, std::string>> chats;
    auto stmt = prepare(SQL);
    if (!stmt) return chats;

    while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
        chats.emplace_back(
            sqlite3_column_int64(stmt.get(), 0),
            reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 1))
        );
    }
    return chats;
}
