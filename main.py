#!/usr/bin/env python
import sys
import logging
import json
import os
import secrets
import time

from operator import itemgetter

import boto3
from botocore.exceptions import WaiterError

boto3.set_stream_logger("boto3.resources", logging.INFO)

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)

try:
    DB_IDENTIFIER_LIVE = os.environ["DB_IDENTIFIER_LIVE"]
    DB_IDENTIFIER_STAGING = os.environ["DB_IDENTIFIER_STAGING"]
    DB_INSTANCE_CLASS = os.environ["DB_INSTANCE_CLASS"]
    DB_SUBNET_GROUP_NAME = os.environ["DB_SUBNET_GROUP_NAME"]
except KeyError as error:
    logging.error("Environment variable `{}` is not set".format(error.args[0]))
    sys.exit(1)

random_hex_string = secrets.token_hex(4)
DB_IDENTIFIER_RESTORE = f"restore-{random_hex_string}"
DB_IDENTIFIER_TERMINATE = f"terminate-{random_hex_string}"

rds = boto3.client("rds")
waiter = rds.get_waiter("db_instance_available")


def wait_for_db_instance(db_identifier: str) -> bool:
    """
    This method works in most cases, except for renaming.
    In rename, the DB states are:

    ['active'] --> 'renaming' --> 'active' --> 'rebooting' --> 'active'

    There is an 'active' state in the middle,
    preventing us from waiting for the final active state. :'(

    https://github.com/boto/boto3/issues/609#issuecomment-216999992
    """
    
    logging.info(f"Waiting for {db_identifier} ...")

    found = False
    iteration_cycle = 0
    max_iterations = 10
    while not found and iteration_cycle < max_iterations:
        try:
            waiter.wait(
                DBInstanceIdentifier=db_identifier
            )  # waits for 30 seconds per iteration, max 60 iterations
        except WaiterError:
            # waiter needs to be able to find a db instance to determine state.
            logging.info(f"{db_identifier} not found, sleeping for 30 seconds...")
            iteration_cycle += 1
            time.sleep(30)  # wait for 30 seconds before starting the check
        else:
            found = True
            logging.info(f"DB: {db_identifier} is available.")
    return found


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
        SnapshotType="automated",
    )
    sorted_response = sorted(
        response["DBSnapshots"], key=itemgetter("SnapshotCreateTime"), reverse=True
    )
    snapshot_id = sorted_response[0]["DBSnapshotIdentifier"]
    logging.info(f"Found snapshot with id: {snapshot_id}")
    return snapshot_id


def swap_restore_with_staging():
    """
    This method swaps the temperary restored DB with the staging DB.
    It then deletes the old staging DB.

    :return: Returns a boolean True, on success
    :raises: 
    """

    # wait until restore db has come online
    wait_for_db_instance(DB_IDENTIFIER_RESTORE)

    # rename staging db to terminate so that restore can take its place.
    logging.info(f"Renaming {DB_IDENTIFIER_STAGING} to {DB_IDENTIFIER_TERMINATE}")
    rds.modify_db_instance(
        DBInstanceIdentifier=DB_IDENTIFIER_STAGING,
        NewDBInstanceIdentifier=DB_IDENTIFIER_TERMINATE,
        ApplyImmediately=True,
    )

    # verify staging has been renamed
    wait_for_db_instance(DB_IDENTIFIER_TERMINATE)

    # rename restore to staging
    logging.info(f"Renaming {DB_IDENTIFIER_RESTORE} to {DB_IDENTIFIER_STAGING}")
    rds.modify_db_instance(
        DBInstanceIdentifier=DB_IDENTIFIER_RESTORE,
        NewDBInstanceIdentifier=DB_IDENTIFIER_STAGING,
        ApplyImmediately=True,
    )

    # verify new staging is alive
    wait_for_db_instance(DB_IDENTIFIER_STAGING)

    # cleanup the old staging DB
    logging.info(f"Deleting {DB_IDENTIFIER_TERMINATE}")
    rds.delete_db_instance(
        DBInstanceIdentifier=DB_IDENTIFIER_TERMINATE, SkipFinalSnapshot=True
    )

    return True


def mangle_restore_data():
    """
    1. Wait for the restore DB to come online.
    2. Log in and execute the SQL scripts to obfuscate user data
    3. If the script is succesfull, continue.
    """
    wait_for_db_instance(DB_IDENTIFIER_RESTORE)

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
    logging.info(f"Restoring snapshot {snapshot_id} to db instance identified by {DB_IDENTIFIER_RESTORE}")
    return response


def run():
    snapshot_id = get_latest_snapshot_id_for_db(DB_IDENTIFIER_LIVE)
    restore_from_snapshot(snapshot_id)

    mangle_restore_data()

    swap_restore_with_staging()
    logging.info("success")

    return "success"


if __name__ == "__main__":
    run()
