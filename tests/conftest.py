import pytest
import json
import boto3
from botocore.exceptions import ClientError
from snowflake.snowpark.session import Session



def get_aws_session(aws_config=None, user=None, token_code=None, access_key=None, secret_access_key=None):
    if token_code:
        # verifying local user machine credentials
        client = boto3.client('sts')
        response = client.assume_role(
            RoleArn=f'arn:aws:iam::291621539275:role/{aws_config}',
            RoleSessionName='AWSCLI-Session',
            SerialNumber=f'arn:aws:iam::291621539275:mfa/{user}',
            TokenCode=token_code
        )
        # placing temporary credentials into variables
        access_key_id = response['Credentials']['AccessKeyId']
        secret_access_key = response['Credentials']['SecretAccessKey']
        session_token = response['Credentials']['SessionToken']

        # creating session with temporary credentials using local profile
        session = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token,
            region_name='us-west-2',
            profile_name='role-with-mfa'
        )
        return session
    elif access_key:
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key
        )
        return session
    else:
        session = boto3.Session()
        return session
def get_secret(secret_name, session):
    region_name = 'us-west-2'

    # Create a Secrets Manager client
    secrets_client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code']:
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            pass
    return json.loads(secret)
@pytest.fixture(scope = "session")
def snowflake():
    secrets = get_secret("snowflake_credentials", get_aws_session())
    snowflake_conn_prop = {
        'user': secrets['user'],
        'password': secrets['pass'],
        'account': 'AT38648',
        'warehouse': 'COMPUTE_WH',
        'protocol': 'https'
    }
    session = Session.builder.configs(snowflake_conn_prop).create()

    return session

