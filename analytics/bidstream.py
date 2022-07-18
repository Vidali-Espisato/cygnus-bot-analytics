import os
import json
import logging
import asyncio, random
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError
from analytics.utils import DATE_FORMAT
from analytics.db import aggregate_bidstream_records, create_or_update_bidstream_records, BID_STREAM
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger('bidstream')
records = {}

async def fetch_bidstream(aws_client, log_group_name, date_string, queue, **kwargs):

    logger.info("Fetching from cloudwatch logs...")

    start_time = datetime.strptime(date_string, DATE_FORMAT)
    end_time = start_time + timedelta(hours=23, minutes=59, seconds=59)

    count = 0
    next_token = True
    while next_token:
        query_args = {
            "logGroupName": log_group_name,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            # "limit": 100
        }

        if isinstance(next_token, str):
            query_args["nextToken"] = next_token

        response = aws_client.filter_log_events(**query_args)
        next_token = response.get("nextToken")
        result = response.get("events")
        
        await queue.put(result)
        logger.info(f"Fetched {len(result)} records")

        await asyncio.sleep(random.random())

        count += 1

        if count == 100:
            break

async def parse_bidstream(queue):

    while True:
        data = await queue.get()

        await asyncio.sleep(random.random())

        for record in data:
            try:
                message = json.loads(record.get("message"))
            except JSONDecodeError:
                continue

            imp = message.get("imp")
            if not (imp and isinstance(imp, list)):
                continue
            
            # url = message["site"].get("page")
            domain = message["site"].get("domain")
            geo = message["device"]["geo"].get("country")

            timestamp = record.get("ingestionTime")
            ingested_on = str(datetime.fromtimestamp(timestamp / 1000)).split(" ")[0] if isinstance(timestamp, int) else None

            record_key = f"{ingested_on}|{domain}|{geo}"
            record_data = records.get(record_key, {})
            ad_slots = set(record_data.get("ad_slots", []))
            banner = imp[0].get("banner")
            ad_slots.add("{}x{}".format(banner.get("w", 0), banner.get("h", 0)))

            total_cpm = record_data.get("total_cpm", 0) + imp[0].get("bidfloor", 0)
            bids_count = record_data.get("bids_count", 0) + 1
            
            record_data.update({
                "ad_slots": list(ad_slots),
                "total_cpm": round(total_cpm, 4),
                "bids_count": bids_count
            })
        
            records[record_key] = record_data

        logger.info(f"Parsed {len(data)} records\n")
        queue.task_done()


def aggregate_n_days_records(n=28):

    target_date = (datetime.now() - timedelta(days=n)).strftime('%Y-%m-%d')
    logger.info(f"Aggregating for {n} days. i.e., from {target_date} till today...")

    aggregate_query = [
        {
            "$match": {
                "ingested_on": {
                    "$gte": target_date
                }
            }
        },
        {
            "$unwind": "$ad_slots"
        }, { 
            "$group": { 
                "_id": { 
                    "domain": "$domain", 
                    "geo": "$geo" 
                }, 
                "bids_count": { 
                    "$sum": "$bids_count" 
                }, 
                "total_cpm": { 
                    "$sum": "$total_cpm" 
                }, 
                "ad_slots": { 
                    "$addToSet": "$ad_slots" 
                }
            }
        }, { 
            "$sort": { 
                "bids_count": -1
            }   
        },
        { 
            "$group": {  
                "_id": "$_id.domain", 
                "total_cpm": { 
                    "$sum": "$total_cpm" 
                }, 
                "geo_count": { 
                    "$push": { 
                        "geo": "$_id.geo", 
                        "bids_count": "$bids_count" 
                    }
                }, 
                "ad_slots": { 
                    "$addToSet": "$ad_slots" 
                } 
            }   
        }, { 
            "$project": { 
                "_id": 0, 
                "domain": "$_id", 
                "avg_cpm": { 
                    "$divide": [ 
                        "$total_cpm", { 
                            "$sum": "$geo_count.bids_count"  
                        }   
                    ]   
                }, 
                "ad_slots": { 
                    "$size": "$ad_slots" 
                }, 
                "geo": { 
                    "$slice": [
                        "$geo_count.geo", 
                        5  
                    ]  
                }  
            }  
        }, {
            "$out": BID_STREAM
        }   
    ]

    return aggregate_bidstream_records(aggregate_query)


async def process_bidstream(aggregate_for_n_days=0, **kwargs):

    queue = asyncio.Queue()
    producer = asyncio.create_task(fetch_bidstream(**kwargs, queue=queue))
    consumer = asyncio.create_task(parse_bidstream(queue))

    await asyncio.gather(producer)
    await queue.join()
    consumer.cancel()

    acknowledgement = create_or_update_bidstream_records(records)
    if acknowledgement:
        logger.info(f"Bidstream records -> added: {acknowledgement.upserted_count} | updated: {acknowledgement.modified_count}")
        records.clear()

    if aggregate_for_n_days:
        try:
            aggregate_n_days_records(aggregate_for_n_days)
            logger.info(f"Bidstream records aggregated for {aggregate_for_n_days} days")
        except Exception as e:
            print("Exception at aggregation - bidstream: ", str(e))
