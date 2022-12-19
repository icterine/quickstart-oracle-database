import boto3, json, requests, secrets, string, random, botocore, datetime

def cfn_response(event, context, status, response):
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

def delete_instance(event, context):
    vars = event["ResourceProperties"]
    # Check if instance exist
    client = boto3.client("rds")
    try:
        response = client.describe_db_instances(
            DBInstanceIdentifier=vars["DBInstanceIdentifier"]
        )
    except botocore.exceptions.ClientError  as e:
        if e.response["Error"]["Code"] == "DBInstanceNotFound":
            status = "SUCCESS"
            cfn_response(event, context, status, e)
        else:
            pass
    except Exception as error:
        pass
    print ("Submitting request to delete instance")
    try:
        response = client.delete_db_instance (
            DBInstanceIdentifier=vars['DBInstanceIdentifier'],
            SkipFinalSnapshot=True
        )
    except Exception as e:
        response=e
        status="FAILED"
    else:
        status="SUCCESS"
    cfn_response(event, context, status, "Instance deletion submitted")

def create_instance(event, context):
    vars = event["ResourceProperties"]
    # Extra check to terminate exectution if instance creationin in progress
    client = boto3.client("rds")
    try:
        response = client.describe_db_instances(
            DBInstanceIdentifier=vars["DBInstanceIdentifier"]
        )
        if response['DBInstances']:
            if response['DBInstances'][0]['DBInstanceStatus'] == 'creating':
                print ('Instance provision in progress. Terminating сurrent execution')
    except Exception as e:
        pass
    # Check if source instance exist
    try:
        response = client.describe_db_instances(
            DBInstanceIdentifier=vars["SourceDBInstanceIdentifier"]
        )
        if response['DBInstances']:
            if response['DBInstances'][0]['DBInstanceStatus'] != 'available':
                cfn_response(event,context,"FAILED", "Source instance not avaliable")
        else:
            cfn_response(event,context,"FAILED", "Source instance not avaliable")
    except Exception as e:
        pass
    source_vars=response['DBInstances'][0]
    print (json.dumps(source_vars,default=str, indent=4))
    # Submit request to create instance
    if source_vars['StorageType'] == 'gp2':
        iops = 0
    else:
        iops = int(source_vars['Iops'])
    if source_vars['DeletionProtection'] =='true':
        delete_protection=True
    else:
        delete_protection=False
    try:
        response = client.create_db_instance_read_replica (
            ReplicaMode="mounted",
            SourceDBInstanceIdentifier=vars['SourceDBInstanceIdentifier'],
            DBInstanceIdentifier=vars['DBInstanceIdentifier'],
            DBInstanceClass=source_vars['DBInstanceClass'],
            AutoMinorVersionUpgrade=False,
            CustomIamInstanceProfile=source_vars['CustomIamInstanceProfile'],
            KmsKeyId=source_vars['KmsKeyId'],
            Iops=iops,
            StorageType=source_vars['StorageType'],
            DeletionProtection=delete_protection
        )
        print (response)
    except Exception as e:
        status = "FAILED"
        response = e
        print (e)
    else:
        status="SUCCESS"
        cfn_response(event, context, status, response)
        # Enable scheduled execution of status check lamnda code via EventBridge
        # Start time passed via eventbridge
        print ("Update rule target")
        client = boto3.client("events")
        try:
            response = client.list_targets_by_rule(
                Rule=vars["EventRule"] 
            )
            arn = response["Targets"][0]["Arn"]
            input = json.loads(response["Targets"][0]["Input"])
            input["instance_createtime"] = str(datetime.datetime.now(datetime.timezone.utc))
            response = client.put_targets(
                Rule=vars["EventRule"],
                Targets=[
                    {
                        'Id': 'email',
                        'Arn': str(arn),
                        'Input': json.dumps(input)
                    }
                ]
            )
            print (response)
        except Exception as e:
            print (e)
            print ("Count not update status check. Check status of custom instance manually")
        print ("Enable Rule")
        try:
            response = client.enable_rule( Name=vars["EventRule"] )
            print (response)
        except Exception as e:
            print (e)
            print ("Count not enable status check. Check status of instance manually")
            pass
    

def lambda_handler(event, context):
    print(event)
    if event["RequestType"] == "Delete":
        delete_instance(event, context)
    elif event["RequestType"] == "Update":
        cfn_response(event,context,"FAILED", "RDS Custom Oracle UPDATE is not supported by quick start deployment")
    elif event["RequestType"] == "Create":
        create_instance(event, context)
    else:
        cfn_response(event,context,"FAILED", "No action provided")
