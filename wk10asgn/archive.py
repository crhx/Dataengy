#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Compress messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import zlib
import ccloud_lib


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_2'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Dictonary to map messages to by key:value pair
    msg_dict = {}
    # File to write compressed messages to
    f = open('compressed.txt', 'wb')
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                #No more messages, compress dictonary into the .txt
                compressed_data = zlib.compress(json.dumps(msg_dict).encode('utf-8'))
                f.write(compressed_data)
                print("Wrote compressed data to compressed.txt file.")
                #Break out of try/catch block
                break
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Store messages to dicotnary
                msg_dict[str(msg.key())] = msg.value().decode('utf-8')
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets & close file
        consumer.close()
        f.close()
