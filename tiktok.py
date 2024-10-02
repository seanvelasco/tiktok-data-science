import asyncio
from utils import format_reply, format_comment, format_post

TIKTOK_BASE_URL = "https://www.tiktok.com"
TEMP_VIDEO_BASE_URL = "https://www.tikwm.com"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:129.0) Gecko/20100101 Firefox/129.0"

async def get_video(session, post_id):
    url = f"{TEMP_VIDEO_BASE_URL}/video/media/wmplay/{post_id}.mp4"
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.read()

async def get_post(session, user_id, post_id):
    url = f"{TIKTOK_BASE_URL}/oembed"
    params = {"url": f"{TIKTOK_BASE_URL}/@{user_id}/video/{post_id}"}
    async with session.get(url, params=params) as response:
            response.raise_for_status()
            body =  await response.json()
            return format_post(body)

async def get_comment(session, post_id, count, cursor):
    url = f"{TIKTOK_BASE_URL}/api/comment/list/"
    params = {
        "aweme_id": post_id,
        "count": count,
        "cursor": cursor,
        "os": "mac",
        "region": "US",
        "screen_height": "900",
        "screen_width": "1440",
        "X-Bogus": "DFSzsIVOxrhAN9fbtfB5EX16ZwHH"
    }
    headers={"User-Agent": UA}

    async with session.get(url, params=params, headers=headers) as response:
        response.raise_for_status()
        body =  await response.json()
        return body

async def get_replies(session, post_id, comment_id, count):
    url = f"{TIKTOK_BASE_URL}/api/comment/list/reply"
    params = {
        "comment_id": comment_id,
        "count": count,
        "item_id": post_id
    }
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        body =  await response.json()
        return body
    
async def get_comments_with_replies(session, post_id):
        comments = []
        offset = 0
        limit = 50

        while True:
            post = await get_comment(session, post_id, limit, offset)
            if not post or "comments" not in post:
                break
            comments.extend(post["comments"])
            if post["has_more"] != 1:
                break
            offset += limit

        comments = [format_comment(comment) for comment in comments]

        # below loop is the same as list comprehension above
        # for comment in comments:
        #     if comment["reply_count"] > 0:
        #         replies = get_replies(post_id, comment["id"], comment["reply_count"])
        #         replies = [process_reply(reply) for reply in replies["comments"]]
        #         comment["replies"] = replies

        tasks = []

        for comment in comments:
            if comment["reply_count"] > 0:
                task = asyncio.create_task(get_replies(session, post_id, comment["id"], comment["reply_count"]))
                tasks.append((comment, task))

        for comment, task in tasks:
            replies = await task
            comment["replies"] = [format_reply(reply) for reply in replies["comments"]]

        return comments