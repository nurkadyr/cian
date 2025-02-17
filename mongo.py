from datetime import datetime

from bson import ObjectId

current_year = datetime.utcnow().year


def insert_html_data(db_html, html_content):
    collection = db_html[f"htmlData{current_year}"]
    document = {
        "CreationTime": datetime.utcnow(),
        "Html": html_content
    }
    result = collection.insert_one(document)
    return result


def insert_photo(db_photos, image_base64):
    collection = db_photos[f"photos{current_year}"]

    document = {
        "Id": None,
        "AdId": None,
        "CreationTime": datetime.utcnow(),
        "Image": {
            "$binary": {
                "base64": image_base64,
                "subType": "00"
            }
        },
        "unique": False
    }

    result = collection.insert_one(document)
    return result


def insert_screenshot(db_screenshots, image_base64):
    collection = db_screenshots[f"screenshots{current_year}"]

    document = {
        "Id": None,
        "AdId": None,
        "CreationTime": datetime.utcnow(),
        "Image": {
            "$binary": {
                "base64": image_base64,
                "subType": "00"
            }
        },
        "unique": False
    }

    result = collection.insert_one(document)
    return result


def update_unique_status(db_photos, db_screenshots, collection_name, document_id, product_id, product_file_id):
    collection = None
    if "photos" in collection_name:
        collection = db_photos[f"photos{current_year}"]
    elif "screenshots" in collection_name:
        collection = db_screenshots[f"screenshots{current_year}"]

    if collection is not None:
        document_id = ObjectId(document_id)
        result = collection.update_one({"_id": document_id}, {"$set": {"Id": product_file_id, "AdId": product_id}})
