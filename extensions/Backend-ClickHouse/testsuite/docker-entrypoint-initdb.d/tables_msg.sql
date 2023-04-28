--CREATE DATABASE IF NOT EXISTS uniset;--

CREATE TABLE IF NOT EXISTS uniset.main_messages
(
    timestamp DateTime('UTC') DEFAULT now(),
    time_usec UInt64 Codec(DoubleDelta, LZ4),
    value Float64 Codec(DoubleDelta, LZ4),
    name LowCardinality(String),
    uniset_hid UInt32 Default murmurHash2_32(name),
    name_hid UInt64 Default cityHash64(name),
    msg_hid UInt32 Default murmurHash2_32(concat(name, toString(value))),
	mtype Enum8('Common', 'Info', 'Normal', 'Cauton', 'Warning', 'Alarm', 'Emergency'),
    message LowCardinality(String)
) ENGINE = MergeTree
PARTITION BY toStartOfDay(timestamp)
PRIMARY KEY(timestamp,time_usec,name_hid)
ORDER BY (timestamp,time_usec,name_hid)
TTL timestamp + INTERVAL 90 DAY;
