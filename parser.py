import json
import pyarrow as pa
import pyarrow.parquet as pq

# load json
with open('convo.json') as f:
    convo = json.load(f)

# create dummy lists
sender_list = []
timestamp_list = []
content_list = []

# parse each message and get values
for message in convo['messages']:
    if 'sender_name' in message:
        sender_list.append(message['sender_name'])
    else:
        sender_list.append(None)

    if 'timestamp_ms' in message:
        timestamp_list.append(message['timestamp_ms'])
    else:
        timestamp_list.append(None)

    if 'content' in message:
        content_list.append(message['content'])
    else:
        content_list.append(None)

# add values to a pyarrow data format
data = [
    pa.array(sender_list, type=pa.string()),
    pa.array(timestamp_list, type=pa.timestamp('ms')),
    pa.array(content_list, type=pa.string())
    ]

batch = pa.RecordBatch.from_arrays(data, ['sender', 'timestamp', 'content']) # create table from arrays
table = pa.Table.from_batches([batch]) # write to batch
pq.write_table(table, 'convo.parquet') # write table to parquet file