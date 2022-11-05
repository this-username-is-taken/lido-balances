#!/usr/bin/env python3

from ast import arg
import http.client
import sys
import os
import time
import grpc
import sys
import base64

from google.protobuf.json_format import MessageToDict

from sf.substreams.v1 import substreams_pb2_grpc
from sf.substreams.v1.substreams_pb2 import Request, STEP_IRREVERSIBLE
from sf.substreams.v1.substreams_pb2 import ModuleOutput
from sf.substreams.v1.package_pb2 import Package

jwt_token = os.getenv("SUBSTREAMS_API_TOKEN")
if not jwt_token: raise Error("set SUBSTREAMS_API_TOKEN")
endpoint = "api.streamingfast.io:443"
package_pb = "substreams-erc20-holdings-v0.1.0.spkg"
output_modules = ["store_balance"]
start_block = 15900000
end_block = start_block + 1
account_balances = {}

def substreams_service():
    credentials = grpc.composite_channel_credentials(
        grpc.ssl_channel_credentials(),
        grpc.access_token_call_credentials(jwt_token),
    )
    channel = grpc.secure_channel(endpoint, credentials=credentials)
    return substreams_pb2_grpc.StreamStub(channel)

def print_balances(account_balances):
    print("\n\n\n")
    print("Total accounts: ", len(account_balances))
    for account, balance in account_balances.items():
        print(account, balance)

def main():
    with open(package_pb, 'rb') as f:
        pkg = Package()
        pkg.ParseFromString(f.read())

    service = substreams_service()
    stream = service.Blocks(Request(
        start_block_num=start_block,
        stop_block_num=end_block,
        fork_steps=[STEP_IRREVERSIBLE],
        modules=pkg.modules,
        output_modules=output_modules,
        initial_store_snapshot_for_modules=output_modules
    ))

    start_time = time.time()

    for response in stream:
        # progress message
        if response.progress:
            print("time elapsed: %.2fs" % (time.time() - start_time))
        
        snapshot = MessageToDict(response.snapshot_data)

        if snapshot and snapshot["moduleName"] == output_modules[0]:
            snapshot_deltas = snapshot["deltas"]
            if snapshot_deltas:
                deltas = snapshot_deltas["deltas"]
                for delta in deltas:
                    key = delta["key"]
                    value = base64.b64decode(delta["newValue"])
                    account_balances[key] = value

        # data = MessageToDict(response.data)
        # if data:
        #     for output in data["outputs"]:
        #         if output["name"] == output_modules[0]:
        #             store_deltas = output["storeDeltas"]
        #             if store_deltas:
        #                 deltas = store_deltas["deltas"]
        #                 for delta in deltas:
        #                     key = delta["key"]
        #                     value = base64.b64decode(delta["newValue"])
        #                     account_balances[key] = value
    
    print_balances(account_balances)

main()
