import boto3, json, requests

def cfn_response(event, context, status, response):
    print (response)
    response = json.dumps(response, sort_keys=True, default=str)
    if  status == "SUCCESS":
        data = {"output" : response}
        reason = status
    else:
        data = {"output" : status}
        reason = response
    response_body = {
        "Status": status,
        "PhysicalResourceId": context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data" : data,
        "Reason": reason
    }
    response_body = json.dumps(response_body)
    print("RESPONSE BODY:n" + response_body)
    try:
        req = requests.put( event["ResponseURL"], data=response_body )
    except requests.exceptions.RequestException as e:
        print(e)
        raise
    return  

def create_engine(event, context):
    vars = event["ResourceProperties"]
    print("Getting CEV manifest file")
    client = boto3.client("s3")
    try:
        obj = client.get_object(
            Bucket=vars["S3MediaBucketName"], Key=vars["S3MediaPrefix"] + "/manifest.json"
        )
        manifest = obj["Body"].read().decode("utf-8")
    except Exception as e:
        cfn_response(event,context,"FAILED", "Could not get manifest file")
        return
    client = boto3.client("rds")
    print("Submitting rds custom engine request")
    # Extra check to prevent double cev submission
    try:
        response = client.describe_db_engine_versions(
            Engine="custom-oracle-ee",
            EngineVersion=vars["EngineVersion"],    
            IncludeAll=True
        )
        if response['DBEngineVersions']:
            if response['DBEngineVersions'][0]['Status'] == 'creating':
                print ('Custom enginge creation in progress. Terminating')
                return
    except Exception:
        print ('extra check failed')
        pass
    try:
        response = client.create_custom_db_engine_version(
            Engine="custom-oracle-ee",
            EngineVersion=vars["EngineVersion"],
            DatabaseInstallationFilesS3BucketName=vars["S3MediaBucketName"],
            DatabaseInstallationFilesS3Prefix=vars["S3MediaPrefix"],
            KMSKeyId=vars["KMSKeyId"],
            Description=vars["Description"],
            Manifest=manifest
        )
        print(response)
    except Exception as e:
        status = "FAILED"
        response = e
    else:
        status="SUCCESS"
        cfn_response(event, context, status, response)
        # This section enables scheduled execution of status check lamnda code via EventBridge
        client = boto3.client("events")
        try:
            response = client.enable_rule( Name=vars["EventRule"] )
            print (response)
        except Exception as e:
            print (e)
            print ("Count not enable status check. Check status of custom engine manually")
            pass

def delete_engine(event, context):
    vars = event["ResourceProperties"]
    # Check if engine exist
    client = boto3.client("rds")
    try:
        response = client.describe_db_engine_versions(
            Engine="custom-oracle-ee",
            EngineVersion=vars["EngineVersion"],    
            IncludeAll=True
        )
        if not response['DBEngineVersions']:
            print ("engine not found")
            return
    except Exception:
        print ('extra check failed')
        pass
    print ("Submitting request to delete engine")
    try:
        response = client.delete_custom_db_engine_version (
            Engine="custom-oracle-ee",
            EngineVersion=vars["EngineVersion"]
        )
    except Exception as e:
        response=e
        status="FAILED"
    else:
        status="SUCCESS"
        response =  "Custom engine  deletion submitted"
    cfn_response(event, context, status, response)


def lambda_handler(event, context):
    print(event)
    if event["RequestType"] == "Delete":
        delete_engine(event, context)
    elif event["RequestType"] == "Update":
        cfn_response(event,context,"FAILED", "RDS Custom Oracle UPDATE is not supported by quick start deployment")
    elif event["RequestType"] == "Create":
        create_engine(event, context)
    else:
        cfn_response(event,context,"FAILED", "No action provided")
    return