import re

def get_list(filename):
    with open(filename, "r") as file:
        list = file.read()
        return list.split()

async def fetch_resource(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.read()
    
def format_post(post):
    return {
        "id": post["embed_product_id"],
        "title": post["title"],
        "username": post["author_unique_id"],
        "nickname": post["author_name"],
        "thumbnail_width": post["thumbnail_width"],
        "thumbnail_height": post["thumbnail_height"],
        "thumbnail": post["thumbnail_url"]
    }
        
def format_user(user):
    return {
        "id": user["uid"],
        "username": user["unique_id"],
        "nickname": user["nickname"],
        "bio": user["signature"],
        "region": user["region"],
        "avatar_uri": user["avatar_uri"],
        "avatar": user["avatar_thumb"]["url_list"][0]
    }

def format_comment(comment):
    return {
        "author": format_user(comment["user"]),
        "id": comment["cid"],
        "created": comment["create_time"],
        "likes_count": comment["digg_count"],
        "reply_count": comment["reply_comment_total"],
        "text": comment["text"],
        "liked_by_creator": comment["is_author_digged"]
    }

def format_reply(reply):
    processed_reply =  {
        "author": format_user(reply["user"]),
        "id": reply["cid"],
        "created": reply["create_time"],
        "likes_count": reply["digg_count"],
        "text": reply["text"],
        "liked_by_creator": reply["is_author_digged"],
        "parent": reply["reply_id"]
    }

    if "reply_to_userid" in reply and "reply_to_reply_id" in reply:
        processed_reply["parent_user"] = reply["reply_to_userid"]
        processed_reply["parent"] = reply["reply_to_reply_id"]

    return processed_reply

def threaded_comments_and_replies(comments):
    # comment
    #   reply A
    #   reply B to A
    #   reply C to B
    #   reply D to A
    #   reply E to B 
    #   reply F to C
    
    # comment
    #   reply A
    #       reply B to A
    #           reply C to B
    #               reply F to C
    #        reply D to A
    #           reply E to B
    for comment in comments:
        if "replies" in comment:
            for reply in comment["replies"]:
                if "parent_comment" in reply:
                    # Make this a dict for more efficient lookups
                    parent = next((p for p in comment["replies"] if p.get("id") == reply["parent_comment"]), None)
                    if parent:
                        if "replies" not in parent:
                            parent["replies"] = []
                        parent["replies"].append(reply)
                        comment["replies"].remove(reply)
    return comments

def flatten_comments_and_replies(comments):
    # comment A
    #   reply A
    #   reply B to A
    #   reply C to B
    #   reply D to A
    # comment B
    # comment C

    # comment A
    # reply A
    # comment B
    # reply B to A
    # reply C to B
    # comment C
    # reply D to A
    replies = []
    for comment in comments:
        if "replies" in comment:
            replies.extend(comment["replies"])
            del comment["replies"]
    comments.extend(replies)
    comments = list({comment["id"]: comment for comment in comments}.values()) # im not sure why there are duplicates, but this removes all duplicates
    return comments

def extract_username_and_post_id(url):
    pattern = r'tiktok\.com/@([^/]+)/video/(\d+)'
    match = re.search(pattern, url)
    if match:
        username = match.group(1)
        video_id = match.group(2)
        return username, video_id