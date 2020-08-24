import json, boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

def find_pipeline(pattern):
    dynamodb = boto3.resource("dynamodb", region_name='us-east-1', endpoint_url="http://dynamodb.us-east-1.amazonaws.com")
    table = dynamodb.Table('dttl_golden_image_orders')
    if not pattern:
        print("pattern is not available")
        pattern="linux_aws_hvm64"
    else:
        print(pattern)
    
    try:
        response = table.get_item(
            Key={
                'architecture': pattern
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        item=response['Item']
        return(item['pipeline'])
    
    
def set_source_image_id(pattern, snn):
    dynamodb = boto3.resource("dynamodb", region_name='us-east-1', endpoint_url="http://dynamodb.us-east-1.amazonaws.com")
    table = dynamodb.Table('dttl_golden_image_orders')
    if not pattern:
        print("pattern is not available")
        pattern="linux_aws_hvm64"
    else:
        print(pattern)
        
    try:
        response = table.get_item(
            Key={
                'architecture': pattern
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        item=response['Item']
        print("source image id is: ",item['source_image_id'])
        sid=item['source_image_id']
        pipeline=item['pipeline']
        machine_size=item['machine_size']
        #put the image to a parameter
        try:
            ssm_client = boto3.client('ssm')
          
            ssm_client.put_parameter(
                Name=pipeline+"_source",
                Value=sid,
                Type='String',
                Overwrite=True
            )
            
            ssm_client.put_parameter(
                Name=pipeline+"_arch",
                Value=pattern,
                Type='String',
                Overwrite=True
            )
            
            ssm_client.put_parameter(
                Name=pipeline+"_snn",
                Value=snn,
                Type='String',
                Overwrite=True
            )
            
            ssm_client.put_parameter(
                Name=pipeline+"_ms",
                Value=machine_size,
                Type='String',
                Overwrite=True
            )
            
        except ssm_client.exceptions as e:
            print(e.response['Error']['Message'])
        else:
            return sid

def insert_snn(table, arch, snn):
    print("Insert SNN start")
    dynamodb = boto3.resource("dynamodb", region_name='us-east-1', endpoint_url="http://dynamodb.us-east-1.amazonaws.com")
    tb_pages=dynamodb.Table(table)
    
    response=tb_pages.update_item(
        Key={
            'architecture':arch
            },
        UpdateExpression="SET curr_snn= :var1",
        ExpressionAttributeValues={
            ':var1': snn,
            },
        ReturnValues="UPDATED_NEW"
        )
    return response

def start_pipeline(pipeline):
    ec2client = boto3.client('codepipeline')
    response = ec2client.start_pipeline_execution(name=pipeline)
    return {
        'statusCode': 200,
        'body': json.dumps('GoldenImageContinousPipeline started')
    }
    
    
def lambda_handler(event, context):

    region=event['queryStringParameters']['key1']
    arch=event['queryStringParameters']['key2']
    snn=event['queryStringParameters']['key3']

    insert_result=insert_snn("dttl_golden_image_orders", arch, snn)
    
    sourceid=set_source_image_id(arch, snn)
    pipeline=find_pipeline(arch)

    response=start_pipeline(pipeline)

    return {
        'statusCode': 200,
        'headers': { 'Content-Type': 'application/json' },
        'body': json.dumps({ 'key1': region, 'key2': arch, "key3": snn })
    }
