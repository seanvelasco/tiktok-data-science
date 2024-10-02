import os
import operator as op
import asyncio
import aiohttp
import psycopg
import aioboto3
from tiktok import get_post, get_comments_with_replies, get_video
from utils import extract_username_and_post_id, get_list, flatten_comments_and_replies, threaded_comments_and_replies, fetch_resource

BUCKET = os.environ.get("S3_BUCKET")
ENDPOINT = os.environ.get("S3_ENDPOINT")
ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY")
REGION = os.environ.get("S3_REGION")
DB_CONN = os.environ.get("PG_CONN_STR")

def get_or_create_db():
    with psycopg.connect(DB_CONN) as conn:
        conn.cursor().execute("""
                              CREATE TABLE IF NOT EXISTS users (
                                id TEXT NOT NULL PRIMARY KEY,
                                username TEXT NOT NULL,
                                nickname TEXT,
                                bio TEXT,
                                region TEXT,
                                scraped TIMESTAMP
                              )
                              """)

        conn.cursor().execute("""
                              CREATE TABLE IF NOT EXISTS posts (
                                id TEXT NOT NULL PRIMARY KEY,
                                title TEXT,
                                author TEXT NOT NULL,
                                scraped TIMESTAMP,
                                deleted BOOL
                              )
                              """)  # FOREIGN KEY (author) REFERENCES users(id)

        conn.cursor().execute("""
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

        # todo: create indices like 'CREATE INDEX IF NOT EXISTS idx_comments_id ON comments (id)'
        conn.commit()
        return conn

def insert_all_to_db(conn, users, post, comments):
    users_data = [(user['id'], user['id'], user['nickname'], user['bio'], user['region']) for user in users]
    posts_data = (post['embed_product_id'], post['title'], post['author_unique_id'])
    comments_data = [(comment['id'], post['embed_product_id'], comment['author']['id'], comment['created'], comment['likes_count'], comment['text'], comment['liked_by_creator'], comment.get('parent')) for comment in comments]

    users_query = "INSERT INTO users (id, username, nickname, bio, region) VALUES (%s, %s, %s, %s, %s)"
    post_query = "INSERT INTO posts (id, title, author) VALUES (%s, %s, %s)"
    comments_query = "INSERT INTO comments (id, post, author, created, likes_count, text, liked_by_author, parent) VALUES (%s, %s, %s, to_timestamp(%s), %s, %s, %s, %s)"

    with conn.cursor() as cur:
        cur.executemany(users_query, users_data)
        cur.execute(post_query, posts_data)
        cur.executemany(comments_query, comments_data)

    return conn

async def fetch_and_upload_video(s3, session, post):
    video_key = f"video/{post['embed_product_id']}"
    video = await get_video(session, post['embed_product_id'])
    await s3.put_object(Bucket=BUCKET, Key=video_key, Body=video, ContentType="video/mp4")

async def fetch_and_upload_thumbnail(s3, session, post):
    thumbnail_key = f"thumbnail/{post['embed_product_id']}"
    thumbnail = await fetch_resource(session, post['thumbnail_url'])
    thumbnail_content_type = "image/jpeg"  # todo: get content-type based on user['avatar'] url
    await s3.put_object(Bucket=BUCKET, Key=thumbnail_key, Body=thumbnail, ContentType=thumbnail_content_type)

async def fetch_and_upload_avatar(s3, session, user):
    avatar_key = f"avatar/{user['id']}"
    avatar = await fetch_resource(session, user['avatar'])
    avatar_content_type = "image/jpeg"  # todo: get content-type based on user['avatar'] url
    await s3.put_object(Bucket=BUCKET, Key=avatar_key, Body=avatar, ContentType=avatar_content_type)

async def upload_all_to_s3(s3, session, post, users):
    # how to handle duplicates for both db insertion and s3 uploads
    await fetch_and_upload_video(s3, session, post)
    await fetch_and_upload_thumbnail(s3, session, post)
    await asyncio.gather(*[fetch_and_upload_avatar(s3, session, user) for user in users])

async def process_post(post, users, comments):
    # this is a transaction, if there's an error fetching data or uploading to s3, no db insertions happen
    async with aiohttp.ClientSession() as session:
        try:
            async with aioboto3.Session().client(service_name="s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY, region_name=REGION) as s3:
                upload_all_to_s3(s3, session, post, users)
                with psycopg.connect(DB_CONN) as conn:
                        insert_all_to_db(conn, users, post, comments)
                        conn.commit()
        except Exception as e:
            print(e)

def evaluate_sentiment(text): # todo
    return text

def detect_relevant_content(text, include_list):
    for keyword in include_list:
        if op.contains(text, keyword):
            # todo: return all matches, not only the first match
            # todo: match misspellings, l33tsp34k, etc
            return keyword

def analyze_comment_and_replies(comments):
    include_list = get_list("include.txt")
    for comment in comments:
        if "text" in comment:
            text = comment['text']
            relevant = detect_relevant_content(text.lower(), include_list)
            if relevant:
                print(f"({relevant}) {text}\n")
        if "replies" in comment:
            analyze_comment_and_replies(comment["replies"])

async def main():
    get_or_create_db()
    async with aiohttp.ClientSession() as session:
        for link in get_list("links.txt"):
            (username, post_id) = extract_username_and_post_id(link)
            comments = await get_comments_with_replies(session, post_id)
            flattened_comments = flatten_comments_and_replies(comments)
            threaded_comments = threaded_comments_and_replies(comments)
            post = await get_post(session, username, post_id)
            users = list({comment["author"]["id"]: comment["author"] for comment in comments}.values())

if __name__ == "__main__":
    asyncio.run(main())
