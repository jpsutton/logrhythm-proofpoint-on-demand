#!/usr/bin/env python

import sqlite3
import time
import sys
import asyncio
import json
import datetime
import urllib.parse
import traceback

import pytz
import websockets
from websockets.exceptions import ConnectionClosed

from utils import time_format

# This is the number of messages to process before syncing a timestamp to disk and reporting queue state
MAX_MSG_COUNTER = 10000
MAX_TS = datetime.datetime(1899, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)

# This seems to be a static value for all customers calling the API (see API documentation)
WS_KEY = "SGVsbG8sIHdvcmxkIQ=="

# The API token will be read from the ini file by the main thread
HEADERS = {
    "Authorization": "Bearer %s",
    "Sec-WebSocket-Key": WS_KEY,
    "Sec-WebSocket-Extensions": "client_no_context_takeover; server_no_context_takeover"
}

# Query string params to use when building the PPoD URI
PARAMS = {
    "cid": "YOUR_CID_HERE",
    "type": "message",
}

# This is the PPoD URI represented as an iterable in the same order as returned by urllib.parse.urlparse.
#   It's represented like this because we need to rebuild versions of this URI with different timestamps
#   if we get more than an hour behind.
URI_BASE = [
    "wss",
    "logstream.proofpoint.com:443",
    "/v1/stream",
    ""
]


# Standard DBAPI 2.0 row factory
def dict_factory(cursor, row):
    d = {}

    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]

    return d


def init_db():
    global MAX_TS

    # Connect to the runtime DB
    cfg_db = sqlite3.connect("runtime_cfg.sqlite")
    cfg_db.row_factory = dict_factory
    cursor = cfg_db.cursor()

    # Initialize DB structure
    cursor.execute("CREATE TABLE IF NOT EXISTS cache (name TEXT, value TEXT, PRIMARY KEY (name))")
    cfg_db.commit()

    # Check for and restore MAX_TS from previous session
    ts_restore = cursor.execute("SELECT * FROM cache where name = 'LAST_TIMESTAMP'", ).fetchall()

    if len(ts_restore):
        ts = int(ts_restore[0]['value'].split(".")[0])
        MAX_TS = pytz.UTC.localize(datetime.datetime.fromtimestamp(ts))
        print(f"Restored previous timestamp: {MAX_TS}")

    # Give DB objects back to caller
    return cfg_db, cursor


async def process_message(msg, log_queue):
    global MAX_TS

    # Parse string as JSON
    json_msg = json.loads(msg)

    # Update MAX_TS if needed
    if "ts" in json_msg:
        ts = datetime.datetime.strptime(json_msg['ts'], time_format)
        MAX_TS = max(MAX_TS, ts)

    await log_queue.put(json_msg)


async def handle_queues (internal_queue, external_queue, exit_flag):
    while not exit_flag.value:
        try:
            msg = internal_queue.get_nowait()
        except asyncio.queues.QueueEmpty:
            await asyncio.sleep(.01)
            continue

        external_queue.put(msg)


async def client(log_queue, api_token, exit_flag):
    global HEADERS

    counter = 0

    # This is a process-internal queue for all the async threads to use
    async_queue = asyncio.Queue()

    # Connect to our runtime database (to track state)
    db_conn, cursor = init_db()

    # Setup the API key in the HTTP headers
    HEADERS.update({"Authorization": HEADERS['Authorization'] % api_token})

    # Start an async thread to dump internal queue to multiprocessing queue
    asyncio.ensure_future(handle_queues(async_queue, log_queue, exit_flag))

    # Continue working until the main thread tells us to stop
    while not exit_flag.value:
        uri = await get_uri()
        print(f"Connecting to {uri} ...")

        try:
            async with websockets.connect(uri=uri, extra_headers=HEADERS) as ws:
                print("Connected!")

                # Get and process messages from the websocket asynchronously
                async for message in ws:
                    await process_message(message, async_queue)
                    counter += 1

                    # Write the current MAX_TS to the runtime DB every MAX_MSG_COUNTER messages
                    if counter > MAX_MSG_COUNTER:
                        counter = 0
                        cursor.execute(
                            "INSERT OR REPLACE INTO cache (name, value) VALUES (?, ?)",
                            ("LAST_TIMESTAMP", str(time.mktime(MAX_TS.timetuple())))
                        )
                        db_conn.commit()
                        print(f"{MAX_MSG_COUNTER} additional messages pulled; Log queue contains approximately {log_queue.qsize()} messages.")

                    if exit_flag.value:
                        break

        except KeyboardInterrupt:
            # this exception gets raised if a ctrl+C is pressed; consume the exception
            pass
        except ConnectionClosed:
            print(f"Connection to websocket unexpectedly closed. Retrying connection...")
        except:
            traceback.print_exc()
            sys.stdout.flush()


async def get_uri():
    now = int(time.time())
    ts = int(time.mktime(MAX_TS.timetuple()))

    # If MAX_TS is older than 1 hour from current time, append "sinceTime" query string param
    if now - ts > 3600 and MAX_TS.year > 1899:
        since_timestamp = MAX_TS.strftime(time_format)
        query_string = urllib.parse.urlencode({**PARAMS, **{"sinceTime": since_timestamp}})
    else:
        query_string = urllib.parse.urlencode(PARAMS)

    return urllib.parse.urlunparse(URI_BASE + [query_string, ""])


def ppod_client(log_queue, api_token, exit_flag):
    # Python 3.7+ style...
    #asyncio.run(client(log_queue, api_token, exit_flag))

    # Python 3.6-compatible
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(client(log_queue, api_token, exit_flag))
