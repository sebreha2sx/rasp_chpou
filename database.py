import aiosqlite
from async_lru import alru_cache

DB_PATH = 'sq3.db'
'''
@lllqi_prx | @rasp_chpou_bot #tg
'''
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;") 
        await db.execute("PRAGMA temp_store=MEMORY;")  # tmp RAM
        await db.execute("PRAGMA cache_size=-10000;")  # ch ~10МБ
        await db.execute('CREATE TABLE IF NOT EXISTS chat_settings (chat_id INTEGER PRIMARY KEY, group_name TEXT)')
        await db.execute('''CREATE TABLE IF NOT EXISTS schedule_archive 
                           (id INTEGER PRIMARY KEY AUTOINCREMENT, date_str TEXT, group_name TEXT, 
                            content TEXT, type TEXT, created_at TIMESTAMP)''')
        await db.execute(
            'CREATE TABLE IF NOT EXISTS pinned_messages (chat_id INTEGER, message_id INTEGER, sent_at TIMESTAMP)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_search ON schedule_archive (date_str, group_name)')
        await db.commit()
      
async def save_to_archive(date_str, group_name, content, entry_type):
    async with aiosqlite.connect(DB_PATH) as db:
        if entry_type == 'основное':
            await db.execute('DELETE FROM schedule_archive WHERE date_str = ? AND group_name = ? AND type = ?',
                             (date_str, group_name, 'основное'))
        await db.execute(
            'INSERT INTO schedule_archive (date_str, group_name, content, type, created_at) VALUES (?, ?, ?, ?, datetime("now"))',
            (date_str, group_name, content, entry_type))

        # Удаляем старое (￣へ￣)
        await db.execute('''
            DELETE FROM schedule_archive 
            WHERE date_str NOT IN (
                SELECT date_str FROM (
                    SELECT date_str FROM schedule_archive 
                    GROUP BY date_str ORDER BY MAX(created_at) DESC LIMIT 45))
        ''')
        await db.commit()


async def get_full_archive_dump():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
                "SELECT date_str, group_name, content, type FROM schedule_archive ORDER BY created_at ASC") as cursor:
            return await cursor.fetchall()


async def add_pinned_msg(chat_id, message_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO pinned_messages VALUES (?, ?, datetime("now"))', (chat_id, message_id))
        await db.commit()


async def get_last_pin_for_chat(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT message_id FROM pinned_messages WHERE chat_id = ? ORDER BY sent_at DESC LIMIT 1',
                              (chat_id,)) as cursor:
            res = await cursor.fetchone()
            return res[0] if res else None


async def get_old_pins():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
                'SELECT chat_id, message_id FROM pinned_messages WHERE sent_at <= datetime("now", "-36 hours")') as cursor:
            rows = await cursor.fetchall()
        await db.execute('DELETE FROM pinned_messages WHERE sent_at <= datetime("now", "-36 hours")')
        await db.commit()
        return rows

@alru_cache(maxsize=5000)
async def get_group(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT group_name FROM chat_settings WHERE chat_id = ?', (chat_id,)) as cursor:
            res = await cursor.fetchone()
            return res[0] if res else None


async def set_group(chat_id, group_name):
    get_group.cache_invalidate(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO chat_settings VALUES (?, ?)', (chat_id, group_name))
        await db.commit()


async def delete_chat(chat_id):
    get_group.cache_invalidate(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM chat_settings WHERE chat_id = ?', (chat_id,))
        await db.execute('DELETE FROM pinned_messages WHERE chat_id = ?', (chat_id,))
        await db.commit()


async def get_all_chats():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT chat_id, group_name FROM chat_settings') as cursor:
            return await cursor.fetchall()
