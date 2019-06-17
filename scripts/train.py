import time
import argparse
import json
from pathlib import Path

import boto3
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('--stage', type=str, default="dev")
args = parser.parse_args()

SEVICE_BASE = "serverless-personalize-example"
STAGE = args.stage
SEVICE_NAME = f"{SEVICE_BASE}-{STAGE}"

stack = boto3.resource('cloudformation').Stack(SEVICE_NAME)
outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.outputs}

S3_BUCKET_NAME = outputs["S3BucketName"]
IAM_ROLE_ARN = outputs["IAMRoleArn"]

DATASET_GRP_NAME = f"{SEVICE_NAME}-dataset-group"
SCHEMA_NAME = f"{SEVICE_NAME}-schema"
DATASET_NAME = f"{SEVICE_NAME}-dataset"
IMPORT_JOB_NAME = f"{SEVICE_NAME}-import-job"
SOLUTION_NAME = f"{SEVICE_NAME}-solution"
CAMPAIGN_NAME = f"{SEVICE_NAME}-campaign"

personalize = boto3.client('personalize')


def get_dataset_grp():
    dataset_groups = personalize.list_dataset_groups()["datasetGroups"]
    dataset_group_names = [dataset_group['name'] for dataset_group in dataset_groups]
    if DATASET_GRP_NAME in dataset_group_names:
        dataset_group_arn = [dataset_group['datasetGroupArn'] for dataset_group in dataset_groups if dataset_group['name'] == DATASET_GRP_NAME][0]
    else:
        dataset_group_arn = create_dataset_group()

    return dataset_group_arn

def create_dataset_group():
    create_dataset_group_response = personalize.create_dataset_group(
        name = DATASET_GRP_NAME
    )
    dataset_group_arn = create_dataset_group_response['datasetGroupArn']

    return dataset_group_arn

def get_schema():
    schemas = personalize.list_schemas()["schemas"]
    schema_names = [schema['name'] for schema in schemas]
    if SCHEMA_NAME in schema_names:
        schema_arn = [schema['schemaArn'] for schema in schemas if schema['name'] == SCHEMA_NAME][0]
    else:
        schema_arn = create_schema()

    return schema_arn

def create_schema():
    schema_path = Path(__file__).parent/"schema.json"
    schema = open(schema_path).read()

    create_schema_response = personalize.create_schema(
        name = SCHEMA_NAME,
        schema = json.dumps(schema)
    )

    schema_arn = create_schema_response['schemaArn']

    return schema_arn

def upload_file():
    src_file = Path(__file__).parents[1]/"data/u.data"
    filename = f"{SEVICE_NAME}-data.csv"
    dist_file = Path(__file__).parents[1]/"data"/filename
    data = pd.read_csv(src_file, sep='\t', names=['USER_ID', 'ITEM_ID', 'RATING', 'TIMESTAMP'])

    # Amazon Personalizeに合わせた形に整形
    data["EVENT_VALUE"] = data["RATING"]
    data["EVENT_TYPE"] = "rating"
    data = data[['USER_ID', 'ITEM_ID', 'EVENT_TYPE', 'EVENT_VALUE', 'TIMESTAMP']]

    # csvファイルとして書き出し
    data.to_csv(dist_file, index=False)

    # アップロード
    boto3.Session().resource('s3').Bucket(S3_BUCKET_NAME).Object(filename).upload_file(str(dist_file))
    data_location = f"s3://{S3_BUCKET_NAME}/{filename}"

    return data_location

def get_dataset(dataset_group_arn, schema_arn):
    datasets = personalize.list_datasets()["datasets"]
    dataset_names = [dataset['name'] for dataset in datasets]
    if DATASET_NAME in dataset_names:
        dataset_arn = [dataset['datasetArn'] for dataset in datasets if dataset['name'] == DATASET_NAME][0]
    else:
        dataset_arn = create_dataset(dataset_group_arn, schema_arn)

    return dataset_arn

def create_dataset(dataset_group_arn, schema_arn):
    dataset_type = "INTERACTIONS"

    create_dataset_response = personalize.create_dataset(
        name = DATASET_NAME,
        datasetType = dataset_type,
        datasetGroupArn = dataset_group_arn,
        schemaArn = schema_arn
    )

    dataset_arn = create_dataset_response['datasetArn']

    return dataset_arn

def get_dataset_job(dataset_arn, data_location):
    jobs = personalize.list_dataset_import_jobs()["datasetImportJobs"]
    job_names = [job['jobName'] for job in jobs]
    if IMPORT_JOB_NAME in job_names:
        return None
    else:
        dataset_import_job_arn = create_dataset_job(dataset_arn, data_location)
        return dataset_import_job_arn

def create_dataset_job(dataset_arn, data_location):
    create_dataset_import_job_response = personalize.create_dataset_import_job(
        jobName = IMPORT_JOB_NAME,
        datasetArn = dataset_arn,
        dataSource = {
            "dataLocation": data_location
        },
        roleArn = IAM_ROLE_ARN
    )

    dataset_import_job_arn = create_dataset_import_job_response['datasetImportJobArn']

    return dataset_import_job_arn

def wait_import_job(dataset_import_job_arn):
    max_time = time.time() + 3*60*60 # 3 hours
    while time.time() < max_time:
        describe_dataset_import_job_response = personalize.describe_dataset_import_job(
            datasetImportJobArn = dataset_import_job_arn
        )
        status = describe_dataset_import_job_response["datasetImportJob"]['status']
        print(f"DatasetImportJob: {status}")

        if status == "ACTIVE" or status == "CREATE FAILED":
            break

        time.sleep(60)

def create_solution(dataset_group_arn):
    recipe_arn = "arn:aws:personalize:::recipe/aws-hrnn"
    create_solution_response = personalize.create_solution(
        name = SOLUTION_NAME,
        datasetGroupArn = dataset_group_arn,
        recipeArn = recipe_arn
    )

    solution_arn = create_solution_response['solutionArn']
    
    return solution_arn

def create_solution_version(solution_arn):
    create_solution_version_response = personalize.create_solution_version(
        solutionArn = solution_arn
    )

    solution_version_arn = create_solution_version_response['solutionVersionArn']

    return solution_version_arn

def wait_create_solution_version(solution_version_arn):
    max_time = time.time() + 3*60*60 # 3 hours
    while time.time() < max_time:
        describe_solution_version_response = personalize.describe_solution_version(
            solutionVersionArn = solution_version_arn
        )
        status = describe_solution_version_response["solutionVersion"]["status"]
        print(f"SolutionVersion: {status}")
        
        if status == "ACTIVE" or status == "CREATE FAILED":
            break
            
        time.sleep(60)

def get_solution(dataset_group_arn):
    solutions = personalize.list_solutions()['solutions']
    solution_names = [solution['name'] for solution in solutions]
    if SOLUTION_NAME in solution_names:
        solution_arn = [solution['solutionArn'] for solution in solutions if solution['name'] == SOLUTION_NAME][0]
    else:
        solution_arn = create_solution(dataset_group_arn)

    return solution_arn

def create_campaign(solution_version_arn):
    create_campaign_response = personalize.create_campaign(
        name = CAMPAIGN_NAME,
        solutionVersionArn = solution_version_arn,
        minProvisionedTPS = 1
    )

    campaign_arn = create_campaign_response['campaignArn']

    return campaign_arn

def wait_create_campaign(campaign_arn):
    max_time = time.time() + 3*60*60 # 3 hours
    while time.time() < max_time:
        describe_campaign_response = personalize.describe_campaign(
            campaignArn = campaign_arn
        )
        status = describe_campaign_response["campaign"]["status"]
        print(f"Campaign: {status}")
        
        if status == "ACTIVE" or status == "CREATE FAILED":
            break
            
        time.sleep(60)

def main():
    # データセットデータグループ作成
    print("prepare dataset group")
    dataset_group_arn = get_dataset_grp()

    # データセット作成
    print("prepare dataset")
    schema_arn = get_schema()
    data_location = upload_file()
    dataset_arn = get_dataset(dataset_group_arn, schema_arn)
    dataset_import_job_arn = get_dataset_job(dataset_arn, data_location)
    if dataset_import_job_arn:
        wait_import_job(dataset_import_job_arn)

    # ソリューション作成
    print("create solution")
    solution_arn = get_solution(dataset_group_arn)
    solution_version_arn = create_solution_version(solution_arn)
    wait_create_solution_version(solution_version_arn)

    # キャンペーン作成
    print("create campaign")
    campaign_arn = create_campaign(solution_version_arn)

    print("training finished!")
    print("campaign_arn:", campaign_arn)

if __name__ == "__main__":
    main()