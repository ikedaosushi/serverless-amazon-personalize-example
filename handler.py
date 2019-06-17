import json
import os

import boto3

CAMPAIGN_ARN = os.environ.get("CAMPAIGN_ARN")

def get_reccomendation(event, context):
    user_id = event['pathParameters']['user_id'] # string

    personalize_runtime = boto3.client('personalize-runtime')
    recommendations_response = personalize_runtime.get_recommendations(
        campaignArn = CAMPAIGN_ARN,
        userId = user_id # stringでない場合はcastする必要がある
    )

    item_list = recommendations_response['itemList']

    response = {
        "statusCode": 200,
        "body": json.dumps(item_list)
    }

    return response

