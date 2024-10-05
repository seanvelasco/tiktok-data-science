import os
import json
import operator as op
import asyncio
import aiohttp
import aioboto3
from dotenv import load_dotenv
from tiktok import TikTok
from detection import detect_relevant_content
from db import get_or_create_db
from utils import extract_mime_type, get_list, fetch_resource, topological_sort

load_dotenv()

BUCKET = os.environ.get("S3_BUCKET")
ENDPOINT = os.environ.get("S3_ENDPOINT")
ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY")
REGION = os.environ.get("S3_REGION")
DB_CONN = os.environ.get("PG_CONN_STR")

def get_all_items_in_db(conn):
    with conn.cursor() as cur:
        result = cur.execute("SELECT id FROM posts").fetchall()
        return [row[0] for row in result]

def insert_all_to_db(conn, users, post, comments):
    users_data = [(user['id'], user['username'], user['nickname'], user['bio'], user['region']) for user in users]
    posts_data = (post['id'], post['title'], post['author']['id'], post['width'], post['height'], post.get('format'), post['duration'], post['likes_count'], post['plays_count'], post['reposts_count'], post['shares_count'], post['created'])
    comments_data = [(comment['id'], post['id'], comment['author']['id'], comment['created'], comment['likes_count'], comment['text'], comment['liked_by_creator'], comment.get('parent')) for comment in comments]

    users_query = "INSERT INTO users (id, username, nickname, bio, region) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING"
    post_query = "INSERT INTO posts (id, title, author, width, height, format, duration, likes_count, plays_count, reposts_count, shares_count, created) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s)) ON CONFLICT (id) DO NOTHING"
    comments_query = "INSERT INTO comments (id, post, author, created, likes_count, text, liked_by_author, parent) VALUES (%s, %s, %s, to_timestamp(%s), %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING"

    with conn.cursor() as cur:
        cur.executemany(users_query, users_data)
        cur.execute(post_query, posts_data)
        cur.executemany(comments_query, comments_data)
    return conn

async def upload_video(s3, post, video):
    await s3.put_object(Bucket=BUCKET, Key=f"video/{post['id']}", Body=video, ContentType="video/mp4")

async def fetch_and_upload_thumbnail(s3, session, post):
    thumbnail_key = f"thumbnail/{post['id']}"
    thumbnail = await fetch_resource(session, post['thumbnail'])
    thumbnail_content_type = "image/png"
    await s3.put_object(Bucket=BUCKET, Key=thumbnail_key, Body=thumbnail, ContentType=thumbnail_content_type)

async def fetch_and_upload_avatar(s3, session, user):
    avatar_key = f"avatar/{user['id']}"
    avatar = await fetch_resource(session, user['avatar'])
    avatar_content_type = extract_mime_type(user['avatar'])
    await s3.put_object(Bucket=BUCKET, Key=avatar_key, Body=avatar, ContentType=avatar_content_type)

async def upload_all_to_s3(s3, session, post, users, video):
    await upload_video(s3, post, video)
    await fetch_and_upload_thumbnail(s3, session, post)
    await asyncio.gather(*[fetch_and_upload_avatar(s3, session, user) for user in users])

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
    async with aiohttp.ClientSession() as session:
        tiktok = TikTok(session)
        with get_or_create_db() as conn:
            async with aioboto3.Session().resource(service_name="s3",endpoint_url=ENDPOINT,aws_access_key_id=ACCESS_KEY_ID,aws_secret_access_key=SECRET_ACCESS_KEY,region_name=REGION) as s3:
            # async with s3.client(service_name="s3",endpoint_url=ENDPOINT,aws_access_key_id=ACCESS_KEY_ID,aws_secret_access_key=SECRET_ACCESS_KEY,region_name=REGION)
                with open("video_items.json", "r") as file:
                    posts = json.loads(file.read())
                    include_list = get_list("include.txt")
                    deny_list = get_list("do_not_include.txt")
                    already_processed = get_all_items_in_db(conn)
                    posts = [post for post in posts if post['id'] not in already_processed and len(detect_relevant_content(post['title'], include_list, deny_list)) != 0]
                    for post in posts:
                        if post['id'] not in already_processed:
                                try:
                                    # video = await tiktok.get_video(post['id'])
                                    comments = await tiktok.get_comments_replies(post['id'], "flat")
                                    comments = topological_sort(comments)
                                    users = list({comment["author"]["id"]: comment["author"] for comment in comments}.values())
                                    # await upload_all_to_s3(s3, session, post, users, video)
                                    insert_all_to_db(conn, users, post, comments)
                                    conn.commit()
                                    print("Processed", post['id'])
                                except Exception as e:
                                    print("Error processing", post['id'], e)
            # with open("comments.json", "w") as file:
            #     file.write(json.dumps(comments, indent=4, ensure_ascii=False))
if __name__ == "__main__":
    asyncio.run(main())
