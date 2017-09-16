import boto3
import operator
#import datetime
from datetime import datetime, timedelta

#AWS Account
ACCOUNT = 'XXXXXXXXXXXX'

#RDS instance
INSTANCE = 'sql2012'

#Type of snapshot to be copied
SNAPTYPE = 'automated'

#Source Region
SOURCEREGION = 'us-east-2'

#Target Region
TARGETREGION = 'eu-central-1'

#Retention Days
RETENTIONDAYS = 7

def copy_latest_snapshot():
    client = boto3.client('rds', SOURCEREGION)
    frankfurt_client = boto3.client('rds', TARGETREGION)

    response = client.describe_db_snapshots(
        DBInstanceIdentifier=INSTANCE,
        SnapshotType= SNAPTYPE,
        IncludeShared=False,
        IncludePublic=False
    )

    if len(response['DBSnapshots']) == 0:
        raise Exception("No" + SNAPTYPE + "snapshots found")

    snapshots_per_project = {}
    for snapshot in response['DBSnapshots']:
        if snapshot['Status'] != 'available':
            continue

        if snapshot['DBInstanceIdentifier'] not in snapshots_per_project.keys():
            snapshots_per_project[snapshot['DBInstanceIdentifier']] = {}

        snapshots_per_project[snapshot['DBInstanceIdentifier']][snapshot['DBSnapshotIdentifier']] = snapshot[
            'SnapshotCreateTime']

    for project in snapshots_per_project:
        sorted_list = sorted(snapshots_per_project[project].items(), key=operator.itemgetter(1), reverse=True)

        copy_name = project + "-" + sorted_list[0][1].strftime("%Y-%m-%d")

        print("Checking if " + copy_name + " is copied")

        try:
            frankfurt_client.describe_db_snapshots(
                DBSnapshotIdentifier=copy_name
            )
        except:
            response = frankfurt_client.copy_db_snapshot(
                SourceDBSnapshotIdentifier='arn:aws:rds:' + SOURCEREGION + ':' + ACCOUNT + ':snapshot:' + sorted_list[0][0],
                TargetDBSnapshotIdentifier=copy_name,
                CopyTags=True
            )

            if response['DBSnapshot']['Status'] != "pending" and response['DBSnapshot']['Status'] != "available":
                raise Exception("Copy operation for " + copy_name + " failed!")
            print("Copied " + copy_name)

            continue

        print("Already copied")


def remove_old_snapshots():
    client = boto3.client('rds', SOURCEREGION)
    frankfurt_client = boto3.client('rds', TARGETREGION)

    response = frankfurt_client.describe_db_snapshots(
        DBInstanceIdentifier=INSTANCE,
        SnapshotType='manual'
    )
    
    if len(response['DBSnapshots']) == 0:
        raise Exception("No manual snapshots in " + TARGETREGION + " found")
    
    snapshots_per_project = {}
    for snapshot in response['DBSnapshots']:
        if snapshot['Status'] != 'available':
            print("Snapshot "+ snapshot['DBSnapshotIdentifier'] + " status is " + snapshot['Status'])
            continue
        
        RETENTIONCUTOFF = datetime.today() - timedelta(days=RETENTIONDAYS)
        if snapshot['SnapshotCreateTime'].replace(tzinfo=None) < RETENTIONCUTOFF :
            print (snapshot['DBSnapshotIdentifier'] + " with timestamp " + str(snapshot['SnapshotCreateTime']) + " is older than " + str(RETENTIONCUTOFF) )
            print("Removing " + snapshot['DBInstanceIdentifier'])
            frankfurt_client.delete_db_snapshot(
            DBSnapshotIdentifier=snapshot['DBInstanceIdentifier']
            )        


def lambda_handler(event, context):
    copy_latest_snapshot()
    remove_old_snapshots()
