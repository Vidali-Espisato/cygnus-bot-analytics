from datetime import datetime
from analytics.utils import DATE_FORMAT
from analytics.db import (RE_DATABASE, RE_COLLECTION,
    re_m_client,
    create_or_update_taxonomy_count_document,
    create_or_update_intent_count_document,
    create_or_update_urls_count_document
)

db = re_m_client[RE_DATABASE]
LANGUAGE_OPTIONS = ["en", "es"]


def get_taxonomy_report():

    aggregate_query = [
        {
            "$unwind": "$taxonomy"
        }, {
            "$group": {
                "_id": {
                    "taxonomy": "$taxonomy.label",
                    "lang": "$lang"
                }, 
                "total": {
                    "$sum": 1
                }
            }
        }, 
        {
            "$group": {
                "_id": "$_id.taxonomy",
                "total": {
                    "$push": {
                        "lang": { "$ifNull": [ "$_id.lang", "" ] },
                        "count": "$total"
                    }
                }
            }
        }
    ]

    db_response = db[RE_COLLECTION].aggregate(aggregate_query)
    languages = set([])
    report = {}

    for record in db_response:

        parent_taxonomy, *extras = record["_id"].split("_")
        child_taxonomy = "_".join(extras)

        if parent_taxonomy not in report:
            report[parent_taxonomy] = {
                "taxonomy": parent_taxonomy,
                "total": {},
                "sub_categories": []
            }

        temp_total = report[parent_taxonomy].get("total", {})

        for item in record["total"]:
            lang = item["lang"]
            if lang not in temp_total:
                temp_total[lang] = 0
                languages.add(lang)

            temp_total[lang] += item["count"]

        report[parent_taxonomy]["total"] = temp_total

        if not child_taxonomy:
            continue

        report[parent_taxonomy]["sub_categories"].append({
            "taxonomy": child_taxonomy,
            "total": record["total"]
        })

    for item in report.values():
        item["sub_categories"] = sorted(item["sub_categories"], key=lambda category: category["taxonomy"])
        item["total"] = [{"lang": lang, "count": _count} for lang, _count in item["total"].items()]

    today = datetime.strptime(datetime.today().date().isoformat(), DATE_FORMAT) 
    document = {
        "date": today, 
        "taxonomies": [item[1] for item in sorted(report.items())],
        "languages": [language for language in languages if language.strip()]
    }

    return create_or_update_taxonomy_count_document(document)


def get_intent_report():

    aggregate_query = [
        {
            "$unwind": "$intent"
        }, {
            "$group": {
                "_id": {
                    "intent": "$intent", 
                    "lang": "$lang"                    
                }, 
                "total": {
                    "$sum": 1
                }
            }
        }, {
            "$group": {
                "_id": "$_id.intent", 
                "total": {
                    "$push": {
                        "lang": {"$ifNull": ["$_id.lang", ""]}, 
                        "count": "$total"
                    }
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'intent': '$_id', 
                'total': '$total'
            }
        }
    ]

    db_response = db[RE_COLLECTION].aggregate(aggregate_query)
    today = datetime.strptime(datetime.today().date().isoformat(), DATE_FORMAT) 
    document = {
        "date": today, 
        "intents": list(db_response)
    }

    return create_or_update_intent_count_document(document)


def get_urls_per_domain_report():

    aggregate_query = [
        {
            "$group": {
                "_id": "$domain", 
                "total": { "$sum": 1 }
            }
        }, {
            '$project': {
                '_id': 0,
                'domain_name': '$_id', 
                'urls_count': '$total'
            }
        }
    ]

    db_response = db[RE_COLLECTION].aggregate(aggregate_query)
    today = datetime.strptime(datetime.today().date().isoformat(), DATE_FORMAT) 
    document = {
        "date": today, 
        "domains": list(db_response)
    }

    return create_or_update_urls_count_document(document)
