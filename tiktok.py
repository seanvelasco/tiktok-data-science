import asyncio
import urllib.parse
from utils import format_reply, format_comment, format_post, flatten_comments_and_replies, threaded_comments_and_replies

class TikTok:
    TIKTOK_BASE_URL = "https://www.tiktok.com"
    TEMP_VIDEO_BASE_URL = "https://www.tikwm.com"
    UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:129.0) Gecko/20100101 Firefox/129.0"

    def __init__(self, session):
        self.session = session

    async def __generate_x_bogus(self, url, UA):
        return url + UA

    async def __get_video_list(self, sec_uid=None, count=35, cursor=0):
        url = f"{self.TIKTOK_BASE_URL}/api/post/item_list/"
        params = {
            "WebIdLastTime": "1727965835",
            "aid": "1988",
            "app_language": "en",
            "app_name": "tiktok_web",
            "browser_language": "en-US",
            "browser_name": "Mozilla",
            "browser_online": "true",
            "browser_platform": "MacIntel",
            "browser_version": "5.0 (Macintosh)",
            "channel": "tiktok_web",
            "cookie_enabled": "true",
            "count": count,
            "coverFormat": "2",
            "cursor": cursor,
            "data_collection_enabled": "true",
            "device_id": "7421556715856102919",
            "device_platform": "web_pc",
            "focus_state": "true",
            "from_page": "user",
            "history_len": "3",
            "is_fullscreen": "false",
            "is_page_visible": "true",
            "language": "en",
            "odinId": "7421556673385858055",
            "os": "mac",
            "priority_region": "",
            "referer": "",
            "region": "PH",
            "screen_height": "900",
            "screen_width": "1440",
            "secUid": sec_uid,
            "tz_name": "Asia/Manila",
            "user_is_login": "false",
            "verifyFp": "verify_m1tjjc15_8GAgXBWe_A5OJ_49He_BoBu_AXRLwobf9DjO",
            "webcast_language": "en",
            "msToken": "0ytO7DW45fJaZ_q4dFfjQB3DwuO-7lyt2ioiDv8fyUWg27bcOw94JOPO5dszm-wR3ktvHL57h4mX9H8Anqp18Ho2ShSR9yxM5eWxEqBL_ae7LOs8wrBpCGhpZ-vBD9jE_4Fxpu-W4Dv5hmG9UabpTdw"
        }

        query_string = urllib.parse.urlencode(params)

        params["X-Bogus"] = self.__generate_x_bogus(f"{self.TIKTOK_BASE_URL}/api/post/item_list/?{query_string}", self.UA)

        async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                return await response.json()

    async def __get_all_video_list_recursive(self):
        video_list = self.__get_video_list()
        if video_list["hasMore"]:
            return
        else:
            self.__get_all_video_list_recursive(28, video_list["cursor"])

    async def get_video_list(self, sec_uid):
        all_video_list = []

        async def fetch_recursive():
            video_list = self.__get_video_list()
            all_video_list.append(video_list["itemList"])
            if video_list["hasMore"]:
                return
            else:
                self.__get_all_video_list_recursive(28, video_list["cursor"])

        await fetch_recursive()

        return all_video_list

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
            comment = await response.json()
            return comment

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
                    print("No comments object here")
                    break
                if post["comments"] == None:
                    print("NO COMMENTS", post_id)
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
