import csv
import logging
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

ITEMS_POLL_WINDOW_TABLE_NAME = 'table-name'
ITEMS_POLL_WINDOW_KEY = 'item-key'
BOARD_ITEMS_TABLE_NAME = 'table-name'
S3_BUCKET_NAME = 'bucket-name'
CSV_FILENAME = 'csv_items'
ROOT_DIRECTORY = "directory"
CSV_PATH = f'{ROOT_DIRECTORY}/{CSV_FILENAME}'
ARCHIVE_PATH = f'{ROOT_DIRECTORY}/archive'


def get_items(env):
    dynamodb = boto3.resource('dynamodb')
    table_name = f'{env}-{BOARD_ITEMS_TABLE_NAME}'
    table = dynamodb.Table(table_name)
    try:
        response = table.scan()
        items = response['Items']
        logging.info(f'Items retrieved from {table_name}: {len(items)}')
        return items
    except ClientError as e:
        logging.error(f'Error retrieving items from {env}-{BOARD_ITEMS_TABLE_NAME}: {e}')
        return []


def create_csv_and_push_to_s3(env, items):
    csv_file = f'{CSV_FILENAME}.csv'
    key = f'{CSV_PATH}.csv'

    header = ["reportingDivision", "reportingDepartment", "reportingCategory", "reportingSubcategory",
              "reportingDivisionId", "reportingDepartmentId", "reportingCategoryId", "reportingSubcategoryId",
              "reportingRangeId", "reportingRange", "vpnStyle", "vpnSku", "item", "euNumber", "itemDescription",
              "supplierColour", "colour", "brand", "subBrand", "brandOwnBrand", "byAge", "fit", "gender", "size",
              "endUse", "finish", "packSingleSet", "neckShape", "rangeName", "numberInPack", "packedDepth",
              "packedWidth", "packedLength", "packedWeight", "character", "supplierId", "season", "goodBetterBest",
              "vatRate", "countryOfOrigin", "baseSellingPrice", "localCurrency", "directShipping",
              "ffm", "deliveryType", "distributionCode", "returnsPercentage", "containerBdcOrHanging",
              "containerSize", "containerQuantity", "shippingPort", "minimumOrderQuantity",
              "timeStamp", "cupSize", "irelandVatRate"]

    try:
        with open(csv_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            for item in items:
                fields = item["fields"]
                fields.update({'timeStamp': datetime.fromisoformat(item['updatedDate'].replace('Z', '+00:00')).timestamp()})
                row = []
                for column in header:
                    row.append(fields.get(column, ""))
                writer.writerow(row)
    except OSError as e:
        logging.error(f'Error creating CSV file: {e}')
        raise

    s3 = boto3.client('s3')
    bucket_name = f'ret-{env}-{S3_BUCKET_NAME}'
    try:
        s3.upload_file(csv_file, bucket_name, key)
        logging.info(f'CSV file successfully generated and sent to {bucket_name}')
    except ClientError as e:
        logging.error(f'Error uploading CSV file to {bucket_name}: {e}')
        raise


def board_sync(env):
    try:
        logging.basicConfig(level=logging.INFO)
        items = get_items(env)

        if not items:
            logging.info('No updates to process')
            return

        create_csv_and_push_to_s3(env, items)
    except Exception as e:
        logging.error(f'An error occurred during board sync: {e}')

        
board_sync('dev')
