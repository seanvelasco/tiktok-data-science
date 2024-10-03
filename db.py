import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_CONN = os.environ.get("PG_CONN_STR")

def get_or_create_db():
    conn = psycopg.connect(DB_CONN)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT NOT NULL PRIMARY KEY,
                    username TEXT NOT NULL,
                    nickname TEXT,
                    bio TEXT,
                    region TEXT,
                    scraped TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id TEXT NOT NULL PRIMARY KEY,
                    title TEXT,
                    author TEXT NOT NULL,
                    width INT,
                    height INT,
                    scraped TIMESTAMP,
                    deleted BOOL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comments (
                    id TEXT NOT NULL PRIMARY KEY,
                    post TEXT NOT NULL,
                    author TEXT NOT NULL,
                    created TIMESTAMP,
                    text TEXT,
                    likes_count INT,
                    liked_by_author BOOL,
                    parent TEXT,
                    scraped TIMESTAMP,
                    deleted BOOL,
                    FOREIGN KEY (author) REFERENCES users(id),
                    FOREIGN KEY (parent) REFERENCES comments(id),
                    FOREIGN KEY (post) REFERENCES posts(id)
                )
            """)
            
            # Create indices
            cur.execute("CREATE INDEX IF NOT EXISTS idx_comments_id ON comments (id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_posts_id ON posts (id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_id ON users (id)")

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e