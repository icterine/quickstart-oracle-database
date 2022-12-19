import json, boto3, datetime
from logging import exception

def send_notificaiton(sns_topic, subject, message):
    client = boto3.client('sns')
    try:
        response = client.publish(
        TopicArn=sns_topic,
        Message=message,
        Subject=subject,
        )
    except Exception as e:
        print (e)
    return

def disable_event_rule(event_rule):
    client = boto3.client("events")
    try:
        response = client.disable_rule( Name=event_rule )
        return
    except Exception as e:
        print (e)
        return
        
def get_cf_templates_location():
    client = boto3.client('ssm')
    try:
        response = client.get_parameter(
            Name='/rds-custom/oracle/cf-templates-location'
        )
        return response['Parameter']['Value']
    except Exception as e:
        print (e)
        return None

def lambda_handler(event, context):
    sns_topic=event["SNSTopicArn"]
    event_rule=event["EventRule"]
    region=event["Region"]
    if event["Phase"] == 'cev':
        print ("Phase : cev")
        engine_version=event["EngineVersion"]
        client = boto3.client('rds')
        try:
            response = client.describe_db_engine_versions(
            Engine="custom-oracle-ee",
            EngineVersion=engine_version,
            IncludeAll=True
            )  
            print (response)
            if response['DBEngineVersions']:
                cev_createtime = response['DBEngineVersions'][0]['CreateTime']
                cev_checktime =  datetime.datetime.now(datetime.timezone.utc)
                if response['DBEngineVersions'][0]['Status'] == 'available':
                    subject = "AWS Notification - [COMPLETED] Custom Oracle engine"
                    message = "AWS Notification - Custom RDS Oracle custom engine " + engine_version + " created successfully"
                    if get_cf_templates_location() is not None:
                        message = message + "To deploy Oracle Primary instance follow https://" + \
                            region + ".console.aws.amazon.com/cloudformation/home?region=" + \
                            region + "#/stacks/create/review?templateURL=" + get_cf_templates_location() + \
                            "rds-custom-oracle-primary-instance.yaml&param_EngineVersion=" + engine_version    
                elif (abs(cev_checktime - cev_createtime) > datetime.timedelta(hours=3)):
                    subject = "AWS Notification - [FAILED] Custom RDS Oracle custom engine"
                    message = "Creation of custom engine has not finished in 3 hours. Check logs or contact AWS support\n\n" \
                                + json.dumps(response, sort_keys=True, default=str)
                else:
                    return
                send_notificaiton(sns_topic, subject, message)
                disable_event_rule(event_rule)
            else:
                print('no response returned')
                subject = "AWS Notification - [FAILED] Custom RDS Oracle custom engine"
                message = "Status check function could not fetch data. please check logs" 
                send_notificaiton(sns_topic, subject, message)
                disable_event_rule(event_rule)
        except Exception as e:
            print ("could not execute rds client. existing")
            print (e)
            subject = "AWS Notification - [FAILED] Custom RDS Oracle custom engine"
            message = json.dumps(e, sort_keys=True, default=str)
            send_notificaiton(sns_topic, subject, message)
            disable_event_rule(event_rule)
            return
    elif  event["Phase"] == 'primary_instance':
        print ("Phase : primary_instance")
        db_instance_identifier=event["DBInstanceIdentifier"]
        client = boto3.client('rds')
        try:
            response = client.describe_db_instances(
                DBInstanceIdentifier=db_instance_identifier
            )
            if response['DBInstances']:
                instance_createtime = datetime.datetime.fromisoformat(event["instance_createtime"])
                instance_checktime =  datetime.datetime.now(datetime.timezone.utc)
                if response['DBInstances'][0]['DBInstanceStatus'] == 'available':
                    subject = "AWS Notification - [COMPLETED] Custom RDS Oracle primary instance"
                    message = "AWS Notification - Custom RDS Oracle primary instance" + db_instance_identifier + " created successfully"
                    if get_cf_templates_location() is not None:
                        message = message + "To deploy Oracle Standby instance follow https://" + \
                            region + ".console.aws.amazon.com/cloudformation/home?region=" + \
                            region + "#/stacks/create/review?templateURL=" + get_cf_templates_location() + \
                            "rds-custom-oracle-standby-instance.yaml&param_SourceDBInstanceIdentifier=" + db_instance_identifier    
                elif (abs(instance_checktime - instance_createtime) > datetime.timedelta(hours=2)):
                        subject = "AWS Notification - [FAILED] Custom RDS Oracle instance"
                        message = "Creation of custom engine has not finished in 2 hours. Check logs or contact AWS support\n\n" \
                                    + json.dumps(response, sort_keys=True, default=str)
                else:
                    return
                send_notificaiton(sns_topic, subject, message)
                disable_event_rule(event_rule)
            else:
                print('no response returned')
                subject = "AWS Notification - [FAILED] Custom RDS Oracle primary instance"
                message = "Status check function could not fetch data. please check logs" 
                send_notificaiton(sns_topic, subject, message)
                disable_event_rule(event_rule)
        except Exception as e:
            print ("could not execute rds client. existing")
            print (e)
            subject = "AWS Notification - [FAILED] Custom RDS Oracle primary instance"
            message = json.dumps(e, sort_keys=True, default=str)
            send_notificaiton(sns_topic, subject, message)
            disable_event_rule(event_rule)
            return
    elif  event["Phase"] == 'standby_instance':
        print ("Phase : standby_instance")
        db_instance_identifier=event["DBInstanceIdentifier"]
        client = boto3.client('rds')
        try:
            response = client.describe_db_instances(
                DBInstanceIdentifier=db_instance_identifier
            )
            if response['DBInstances']:
                instance_createtime = datetime.datetime.fromisoformat(event["instance_createtime"])
                instance_checktime =  datetime.datetime.now(datetime.timezone.utc)
                if response['DBInstances'][0]['DBInstanceStatus'] == 'available':
                    subject = "AWS Notification - [COMPLETED] Custom RDS Oracle standby instance"
                    message = "AWS Notification - Custom RDS Oracle standby instance" + db_instance_identifier + " created successfully"
                elif (abs(instance_checktime - instance_createtime) > datetime.timedelta(hours=2)):
                        subject = "AWS Notification - [FAILED] Custom RDS Oracle instance"
                        message = "Creation of custom engine has not finished in 2 hours. Check logs or contact AWS support\n\n" \
                                    + json.dumps(response, sort_keys=True, default=str)
                else:
                    return
                send_notificaiton(sns_topic, subject, message)
                disable_event_rule(event_rule)
            else:
                print('no response returned')
                subject = "AWS Notification - [FAILED] Custom RDS Oracle standby instance"
                message = "Status check function could not fetch data. please check logs" 
                send_notificaiton(sns_topic, subject, message)
                disable_event_rule(event_rule)
        except Exception as e:
            print ("could not execute rds client. existing")
            print (e)
            subject = "AWS Notification - [FAILED] Custom RDS Oracle standby instance"
            message = json.dumps(e, sort_keys=True, default=str)
            send_notificaiton(sns_topic, subject, message)
            disable_event_rule(event_rule)
            return
    else:
        return