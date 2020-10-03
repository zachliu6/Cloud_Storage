import boto3
import csv

s3 = boto3.resource('s3',
                    aws_access_key_id='',
                    aws_secret_access_key='')
try:
    s3.create_bucket(Bucket='cloudbucketzliu', CreateBucketConfiguration={
        'LocationConstraint': 'us-west-2'})
except:
    print("this may already exist")
bucket = s3.Bucket("cloudbucketzliu")
bucket.Acl().put(ACL='public-read')
# body = open('path-to-a-file\exp1', 'rb')
# o = s3.Object('datacont-name', 'test').put(Body=body)
# s3.Object('datacont-name', 'test').Acl().put(ACL='public-read')
dyndb = boto3.resource('dynamodb',
                       region_name='us-west-2',
                       aws_access_key_id='',
                       aws_secret_access_key='')

try:
    table = dyndb.create_table(TableName='DataTable',
                               KeySchema=[{'AttributeName': 'PartitionKey', 'KeyType': 'HASH'},
                                          {'AttributeName': 'RowKey', 'KeyType': 'RANGE'}],
                               AttributeDefinitions=[{'AttributeName': 'PartitionKey', 'AttributeType': 'S'},
                                                     {'AttributeName': 'RowKey', 'AttributeType': 'S'}],
                               ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})
except:
    # if there is an exception, the table may already exist.
    table = dyndb.Table("DataTable")
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
#print(table.item_count)

with open('/Users/liuzheng/Desktop/Comp Sci/CS1660/Cloud_Storage_project/experiments.csv', 'rt') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        #print(item)
        body = open('/Users/liuzheng/Desktop/Comp Sci/CS1660/Cloud_Storage_project/datafiles/' + item[3], 'rb')
        s3.Object('cloudbucketzliu', item[3]).put(Body=body)
        md = s3.Object('cloudbucketzliu', item[3]).Acl().put(ACL='public-read')
        url = " https://s3-us-west-2.amazonaws.com/cloudbucketzliu/" + item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                         'description': item[4], 'date': item[2], 'url': url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")
response = table.get_item(
    Key={
        'PartitionKey': 'experiment3',
        'RowKey': '3'})
item = response['Item']
print(item)
#print(response)