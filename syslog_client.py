#!/usr/bin/env python

import queue
import requests
import traceback
import pysyslogclient

from utils import get_field, normalize_time, bytes2kilobytes, join_recipients

LR_SYSLOG_FIELDS = list()

#  Source Defined Parser Configuration
fullyQualifiedBeatName = 'webhookbeat_webhook_ppod'
field_map = {
    'action': {'field': 'filter.disposition'},
    'kilobytes': {'field': 'msg.sizeBytes', 'filter': bytes2kilobytes},
    'protname': {'field': 'connection.protocol'},
    'sender': {'field': 'msg.normalizedHeader.from.0'},
    'recipient': {'field': 'envelope.rcpts', 'filter': join_recipients},
    'serialnumber': {'field': 'msg.normalizedHeader.message-id.0'},
    'sip': {'field': 'connection.ip'},
    'sname': {'field': 'connection.host'},
    'status': {'field': 'filter.routeDirection'},
    'subject': {'field': 'msg.normalizedHeader.subject.0'},
    'tag1': {'field': 'filter.disposition'},
    'tag2': {'field': 'filter.routeDirection'},
    'timestamp.iso8601': {'field': 'ts', 'filter': normalize_time},
    'vendorinfo': {'field': 'filter.actions.0.rule'},
}


def send_logs(syslog_server, log_queue, exit_flag):
    with open("LR_Syslog_Fields.txt", "r") as fields_file:
        for line in fields_file.readlines():
            LR_SYSLOG_FIELDS.append(line.strip())

    while not exit_flag.value:
        try:
            client = pysyslogclient.SyslogClientRFC5424(syslog_server, 514, proto="TCP")
            send_logs_session(client, log_queue, exit_flag)
        except:
            pass
        finally:
            client.close()


# Send logs to Open Collector
def send_logs_session(client, log_queue, exit_flag):  # logs if an array of JSON messages, log if just one
    while not exit_flag.value:
        try:
            record = log_queue.get(timeout=1)

            try:
                # This isn't strictly necessary, but I'm trying to match the output of the OC as closely as possible
                msg = ["./image.binary[1]: "]

                # Populate static fields
                oc_log_static = {
                    'beatname': 'webhookbeat',
                    'device_type': 'webhookbeat',
                    'fullyqualifiedbeatname': fullyQualifiedBeatName,
                    'whsdp': True,
                    'original_message': record
                }

                # populate mapped fields
                for fieldname in LR_SYSLOG_FIELDS:
                    if fieldname in oc_log_static:
                        msg.append(f"{fieldname}={oc_log_static[fieldname]}")
                    elif fieldname in field_map:
                        value = str(get_field(field_map, record, fieldname)).replace("|", "")
                        msg.append(f"{fieldname}={value}")

            except:
                print(f"[send_logs] Error parsing record. Traceback follows...\n{traceback.format_exc()}")
                continue

            try:
                # Send data to syslog
                client.log("|".join(msg), facility=3, severity=5)
            except requests.RequestException as error:
                print(f'[send_logs] Error sending log to syslog. Error details...\n{error}')
                return

        except queue.Empty:
            pass
