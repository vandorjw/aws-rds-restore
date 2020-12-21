#!/usr/bin/env python
import sys
import logging
import json
import os
import secrets

from operator import itemgetter

import boto3
boto3.set_stream_logger('boto3.resources', logging.INFO)

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

try:
    DB_IDENTIFIER_LIVE = os.environ['DB_IDENTIFIER_LIVE']
    DB_IDENTIFIER_STAGING = os.environ['DB_IDENTIFIER_STAGING']
    DB_INSTANCE_CLASS = os.environ['DB_INSTANCE_CLASS']
    DB_SUBNET_GROUP_NAME = os.environ['DB_SUBNET_GROUP_NAME']
except KeyError as error:
    logging.error("Environment variable `{}` is not set".format(error.args[0]))
    sys.exit(1)
    
random_hex_string = secrets.token_hex(4)
DB_IDENTIFIER_RESTORE = f'restore-{random_hex_string}'
DB_IDENTIFIER_TERMINATE = f'terminate-{random_hex_string}'

rds = boto3.client('rds')
waiter = rds.get_waiter('db_instance_available')


def get_latest_snapshot_id_for_db(db_identifier: str) -> str:
    """
    :param db_identifier: RDS database identifier, sometimes known as instance identifier
    :return: snapshot_id (a string) is the RDS snapshot identifier
    """
    
    # By default, a snapshot is made once per day, and kept for 7 days
    # The default max # of snapshots returned by describe_db_snapshots is 100.
    # As long we we have less than 100 snapshot, this should get us the latest snapshot
    response = rds.describe_db_snapshots(
        DBInstanceIdentifier=db_identifier,
        SnapshotType='automated',
    )
    sorted_response = sorted(response['DBSnapshots'], key=itemgetter('SnapshotCreateTime'), reverse=True)
    snapshot_id = sorted_response[0]['DBSnapshotIdentifier']
    logging.info(f"Found snapshot with id: {snapshot_id}")
    return snapshot_id


def swap_restore_with_staging():
    """
    This method swaps the temperary restored DB with the staging DB.
    It then deletes the old staging DB.
    
    :return: Returns a boolean True, on success
    :raises Exception:
    """

    # wait until restore db has come online
    waiter.wait(
        DBInstanceIdentifier=DB_IDENTIFIER_RESTORE
    )  # waits for 30 seconds per iteration, max 60 iterations
    
    # modify staging db identifier so that restore can take its place.
    rds.modify_db_instance(
        DBInstanceIdentifier=DB_IDENTIFIER_STAGING,
        NewDBInstanceIdentifier=DB_IDENTIFIER_TERMINATE,
        ApplyImmediately=True,
    )

    # verify staging has been renamed
    waiter.wait(
        DBInstanceIdentifier=DB_IDENTIFIER_TERMINATE
    )  # waits for 30 seconds per iteration, max 60 iterations
    
    
    # rename restore to staging
    rds.modify_db_instance(
        DBInstanceIdentifier=DB_IDENTIFIER_RESTORE,
        NewDBInstanceIdentifier=DB_IDENTIFIER_STAGING,
        ApplyImmediately=True,
    )

    # verify new staging is alive
    waiter.wait(
        DBInstanceIdentifier=DB_IDENTIFIER_STAGING
    )  # waits for 30 seconds per iteration, max 60 iterations

    # cleanup the old staging DB
    rds.delete_db_instance(
        DBInstanceIdentifier=DB_IDENTIFIER_TERMINATE,
        SkipFinalSnapshot=True
    )

    return True


def mangle_restore_data():
    logging.info("stub, figure out how to do this.")
    pass
   

def restore_from_snapshot(snapshot_id):
    response = rds.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier=DB_IDENTIFIER_RESTORE,
        DBSnapshotIdentifier=snapshot_id,
        DBSubnetGroupName=DB_SUBNET_GROUP_NAME,
        DBInstanceClass=DB_INSTANCE_CLASS,
        MultiAZ=False,
    )


def run():   
    snapshot_id = get_latest_snapshot_id_for_db(DB_IDENTIFIER_LIVE)  
    #restore_from_snapshot(snapshot_id)
    
    mangle_restore_data()
    
    #swap_restore_with_staging()
    logging.info('success')

    return 'success'


if __name__ == "__main__":
    run()
