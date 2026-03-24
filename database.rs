use sqlx::{SqlitePool, Row};
use once_cell::sync::Lazy;
use anyhow::Result;

static DATABASE_PATH: &str = "sqlite://sq3.db";
static MAX_ARCHIVE_ENTRIES: i64 = 45;
static PIN_EXPIRATION_HOURS: i64 = 36;

static INIT_QUERIES: &[&str] = &[
    "PRAGMA journal_mode = WAL;",
    "PRAGMA synchronous = NORMAL;",
    "PRAGMA temp_store = MEMORY;",
    "PRAGMA cache_size = -10000;",
];

static DB_POOL: Lazy<SqlitePool> = Lazy::new(|| {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(SqlitePool::connect(DATABASE_PATH))
        .unwrap()
});

pub async fn init_database() -> Result<()> {
    let db = &*DB_POOL;

    for query in INIT_QUERIES {
        sqlx::query(query).execute(db).await?;
    }

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS chat_settings (
            chat_id INTEGER PRIMARY KEY,
            group_name TEXT
        );

        CREATE TABLE IF NOT EXISTS schedule_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_str TEXT,
            group_name TEXT,
            content TEXT,
            entry_type TEXT,
            created_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS pinned_messages (
            chat_id INTEGER,
            message_id INTEGER,
            sent_at TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_search
        ON schedule_archive (date_str, group_name);
        "#,
    )
    .execute(db)
    .await?;

    Ok(())
}

pub async fn save_schedule_entry(
    date: &str,
    group: &str,
    content: &str,
    entry_type: &str,
) -> Result<()> {
    let db = &*DB_POOL;

    if entry_type == "основное" {
        sqlx::query(
            "DELETE FROM schedule_archive
             WHERE date_str = ? AND group_name = ? AND entry_type = 'основное'",
        )
        .bind(date)
        .bind(group)
        .execute(db)
        .await?;
    }

    sqlx::query(
        "INSERT INTO schedule_archive
         (date_str, group_name, content, entry_type, created_at)
         VALUES (?, ?, ?, ?, datetime('now'))",
    )
    .bind(date)
    .bind(group)
    .bind(content)
    .bind(entry_type)
    .execute(db)
    .await?;

    sqlx::query(&format!(
        r#"
        DELETE FROM schedule_archive 
        WHERE date_str NOT IN (
            SELECT date_str FROM (
                SELECT date_str FROM schedule_archive 
                GROUP BY date_str 
                ORDER BY MAX(created_at) DESC 
                LIMIT {}
            )
        )
        "#,
        MAX_ARCHIVE_ENTRIES
    ))
    .execute(db)
    .await?;

    Ok(())
}

pub async fn fetch_full_archive() -> Result<Vec<(String, String, String, String)>> {
    let db = &*DB_POOL;
    let rows = sqlx::query(
        "SELECT date_str, group_name, content, entry_type
         FROM schedule_archive
         ORDER BY created_at ASC",
    )
    .fetch_all(db)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.get("date_str"),
                r.get("group_name"),
                r.get("content"),
                r.get("entry_type"),
            )
        })
        .collect())
}

pub async fn add_pinned_message(chat_id: i64, message_id: i64) -> Result<()> {
    let db = &*DB_POOL;

    sqlx::query(
        "INSERT INTO pinned_messages (chat_id, message_id, sent_at)
         VALUES (?, ?, datetime('now'))",
    )
    .bind(chat_id)
    .bind(message_id)
    .execute(db)
    .await?;

    Ok(())
}

pub async fn get_last_pinned_message(chat_id: i64) -> Result<Option<i64>> {
    let db = &*DB_POOL;
    Ok(sqlx::query(
        "SELECT message_id FROM pinned_messages 
         WHERE chat_id = ? 
         ORDER BY sent_at DESC 
         LIMIT 1",
    )
    .bind(chat_id)
    .fetch_optional(db)
    .await?
    .map(|r| r.get("message_id")))
}

pub async fn cleanup_old_pins() -> Result<Vec<(i64, i64)>> {
    let db = &*DB_POOL;

    let expired_rows = sqlx::query(
        &format!(
            "SELECT chat_id, message_id FROM pinned_messages
             WHERE sent_at <= datetime('now', '-{} hours')",
            PIN_EXPIRATION_HOURS
        ),
    )
    .fetch_all(db)
    .await?;

    let result = expired_rows
        .iter()
        .map(|r| (r.get("chat_id"), r.get("message_id")))
        .collect::<Vec<_>>();

    sqlx::query(&format!(
        "DELETE FROM pinned_messages
         WHERE sent_at <= datetime('now', '-{} hours')",
        PIN_EXPIRATION_HOURS
    ))
    .execute(db)
    .await?;

    Ok(result)
}

pub async fn get_group_name(chat_id: i64) -> Result<Option<String>> {
    let db = &*DB_POOL;
    Ok(sqlx::query(
        "SELECT group_name FROM chat_settings WHERE chat_id = ?",
    )
    .bind(chat_id)
    .fetch_optional(db)
    .await?
    .map(|r| r.get("group_name")))
}

pub async fn set_group_name(chat_id: i64, group: &str) -> Result<()> {
    let db = &*DB_POOL;

    sqlx::query(
        "INSERT OR REPLACE INTO chat_settings (chat_id, group_name)
         VALUES (?, ?)",
    )
    .bind(chat_id)
    .bind(group)
    .execute(db)
    .await?;

    Ok(())
}

pub async fn delete_chat_data(chat_id: i64) -> Result<()> {
    let db = &*DB_POOL;

    sqlx::query("DELETE FROM chat_settings WHERE chat_id = ?")
        .bind(chat_id)
        .execute(db)
        .await?;

    sqlx::query("DELETE FROM pinned_messages WHERE chat_id = ?")
        .bind(chat_id)
        .execute(db)
        .await?;

    Ok(())
}

pub async fn get_all_chats() -> Result<Vec<(i64, String)>> {
    let db = &*DB_POOL;
    let rows = sqlx::query("SELECT chat_id, group_name FROM chat_settings")
        .fetch_all(db)
        .await?;

    Ok(rows
        .into_iter()
        .map(|r| (r.get("chat_id"), r.get("group_name")))
        .collect())
}
