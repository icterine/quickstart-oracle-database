from logging import exception
import boto3, json, requests



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

# def check_timeout (stack_name, timeout):
#     client = boto3.client('cloudformation')
#     try:
#         response = client.describe_stacks(
#         StackName=stack_name,
#         )
#         print (response['Stacks'][0][LastUpdatedTime])
#         #cf_update_time=
#     except:
#         print ("except")что т
#     return

def deploy_cf (template_url, stack_name, parameters):
    client = boto3.client('cloudformation')
    try:
        response = client.create_stack(
            StackName=stack_name,
            TemplateURL=template_url,
            Parameters=[parameters]
        )
        return (True, response)
    except Exception as e:
        return (False, e)


def lambda_handler(event, context):
    print(event)
    # Check if timeout 
    # Detect phase
    if (event["Phase"]["Current"]) == "Engine":
        # Do we need start deployment
        if (event["Phase"]["StartTime"]) == "null":
            print ("Deploy")
            (success, response) = deploy_cf("s", "s", "s")
            print (success, response)
        else:
            print ("Something wrong. Notify and disable rule")

    #check_timeout(event["CFName"], int(event["Timeout"]))

    return
    if event["RequestType"] == "Delete":
        delete_engine(event, context)
    elif event["RequestType"] == "Update":
        cfn_response(event,context,"FAILED", "RDS Custom Oracle UPDATE is not supported by quick start deployment")
    elif event["RequestType"] == "Create":
        check_timeout('somestack')
    else:
        cfn_response(event,context,"FAILED", "No action provided")
    return

event={}
event["Phase"]={}
event["Phase"]["Current"]="Engine"
event["Phase"]["StartTime"] = "null"



lambda_handler(event, "test")