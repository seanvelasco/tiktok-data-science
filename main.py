import os
import operator as op
import asyncio
import aiohttp
import aioboto3
from dotenv import load_dotenv
from tiktok import TikTok
from utils import extract_username_and_post_id, get_list, fetch_resource
from db import get_or_create_db

load_dotenv()

BUCKET = os.environ.get("S3_BUCKET")
ENDPOINT = os.environ.get("S3_ENDPOINT")
ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY")
REGION = os.environ.get("S3_REGION")

def insert_all_to_db(conn, users, post, comments):
    users_data = [(user['id'], user['username'], user['nickname'], user['bio'], user['region']) for user in users]
    posts_data = (post['id'], post['title'], post['username'], post['width'], post['height'])
    comments_data = [(comment['id'], post['id'], comment['author']['id'], comment['created'], comment['likes_count'], comment['text'], comment['liked_by_creator'], comment.get('parent')) for comment in comments]

    users_query = "INSERT INTO users (id, username, nickname, bio, region) VALUES (%s, %s, %s, %s, %s)"
    post_query = "INSERT INTO posts (id, title, author, width, height) VALUES (%s, %s, %s, %s, %s)"
    comments_query = "INSERT INTO comments (id, post, author, created, likes_count, text, liked_by_author, parent) VALUES (%s, %s, %s, to_timestamp(%s), %s, %s, %s, %s)"

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
    thumbnail_content_type = "image/jpeg"  # todo: get content-type based on user['avatar'] url
    await s3.put_object(Bucket=BUCKET, Key=thumbnail_key, Body=thumbnail, ContentType=thumbnail_content_type)

async def fetch_and_upload_avatar(s3, session, user):
    avatar_key = f"avatar/{user['id']}"
    avatar = await fetch_resource(session, user['avatar'])
    avatar_content_type = "image/jpeg"  # todo: get content-type based on user['avatar'] url
    await s3.put_object(Bucket=BUCKET, Key=avatar_key, Body=avatar, ContentType=avatar_content_type)

async def upload_all_to_s3(s3, session, post, users, video):
    await upload_video(s3, post, video)
    await fetch_and_upload_thumbnail(s3, session, post)
    await asyncio.gather(*[fetch_and_upload_avatar(s3, session, user) for user in users])
async def upload_all(post, users, comments, video):
    # this is a transaction, if there's an error fetching data or uploading to s3, no db insertions happen
    async with aiohttp.ClientSession() as session:
        try:
            async with aioboto3.Session().client(service_name="s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY, region_name=REGION) as s3:
                await upload_all_to_s3(s3, session, post, users, video) # how to handle duplicates for both db insertion and s3 uploads
                with get_or_create_db() as conn:
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
        tiktok = TikTok(session)
        for link in get_list("links.txt"):
            (username, post_id) = extract_username_and_post_id(link)
            comments = await tiktok.get_comments_replies(post_id, "flat")
            video = await tiktok.get_video(post_id)
            post = await tiktok.get_post(username, post_id)
            users = list({comment["author"]["id"]: comment["author"] for comment in comments}.values())

            # with open("comments.json", "w") as file:
            #     file.write(json.dumps(comments, indent=4, ensure_ascii=False))
            # with open("post.json", "w") as file:
            #     file.write(json.dumps(post, indent=4, ensure_ascii=False))
            # with open("users.json", "w") as file:
            #         file.write(json.dumps(users, indent=4, ensure_ascii=False))

            await upload_all(post, users, comments, video)

if __name__ == "__main__":
    asyncio.run(main())