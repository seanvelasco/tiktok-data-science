from itertools import islice

async def list_all(s3, bucket):
    paginator = s3.get_paginator("list_objects_v2")
    items = []
    async for response in paginator.paginate(Bucket=bucket):
        if "Contents" in response:
            for content in response["Contents"]:
                items.append(content["Key"])
    return items

async def delete_objcets(s3, bucket, objects_to_delete):
    responses = []
    batch_size = 1000
    for i in range(0, len(objects_to_delete), batch_size):
        batch = list(islice(objects_to_delete, i, i + batch_size))
        response = await s3.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': [{'Key': key} for key in batch],
                'Quiet': False
            }
        )
        responses.append(response)
    return responses

# sample usage
# all = await list_all(s3, BUCKET)
# removed = await delete_objcets(s3, BUCKET, all)
