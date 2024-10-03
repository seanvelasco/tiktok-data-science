import asyncio
from utils import format_reply, format_comment, format_post, flatten_comments_and_replies, threaded_comments_and_replies

class TikTok:
    TIKTOK_BASE_URL = "https://www.tiktok.com"
    TEMP_VIDEO_BASE_URL = "https://www.tikwm.com"
    UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:129.0) Gecko/20100101 Firefox/129.0"

    def __init__(self, session):
        self.session = session

    async def get_video(self, post_id):
        url = f"{self.TEMP_VIDEO_BASE_URL}/video/media/wmplay/{post_id}.mp4"
        async with self.session.get(url) as response:
            response.raise_for_status()
            return await response.read()

    async def get_post(self, user_id, post_id):
        url = f"{self.TIKTOK_BASE_URL}/oembed"
        params = {"url": f"{self.TIKTOK_BASE_URL}/@{user_id}/video/{post_id}"}
        async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                body =  await response.json()
                return format_post(body)

    async def __get_comment(self, post_id, count, cursor):
        url = f"{self.TIKTOK_BASE_URL}/api/comment/list/"
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
        headers={"User-Agent": self.UA}

        async with self.session.get(url, params=params, headers=headers) as response:
            response.raise_for_status()
            return await response.json()

    async def __get_replies(self, post_id, comment_id, count):
        url = f"{self.TIKTOK_BASE_URL}/api/comment/list/reply"
        params = {
            "comment_id": comment_id,
            "count": count,
            "item_id": post_id
        }
        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()
        
    async def get_comments_replies(self, post_id, format):
            comments = []
            offset = 0
            limit = 50

            while True:
                post = await self.__get_comment(post_id, limit, offset)
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
                    task = asyncio.create_task(self.__get_replies(post_id, comment["id"], comment["reply_count"]))
                    tasks.append((comment, task))

            for comment, task in tasks:
                replies = await task
                comment["replies"] = [format_reply(reply) for reply in replies["comments"]]

            if (format == "flat"):
                return flatten_comments_and_replies(comments)
            elif (format == "thread"):
                return threaded_comments_and_replies(comments)
            else:
                return comments