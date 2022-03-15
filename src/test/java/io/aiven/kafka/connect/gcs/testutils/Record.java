/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.gcs.testutils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

/*
Class that used for fixtures in tests.
Has converter to SinkRecord.
 */
public class Record {
    public String topic;
    public String key;
    public String value;
    public int partition;
    public int offset;
    public long timestamp;
    public Iterable<Header> headers;

    public Record(final String key, final String value, final Iterable<Header> headers) {
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    public Record(final String topic, final String key, final String value, final int partition, final int offset,
            final long timestamp, final Iterable<Header> headers) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    public static Record of(final String key, final String value, final Iterable<Header> headers) { // NOPMD short
                                                                                                    // method name
        return new Record(key, value, headers);
    }

    public static Record of(final String topic, final String key, final String value, final int partition, // NOPMD
                                                                                                           // short
                                                                                                           // method
                                                                                                           // name
            final int offset, final int timestamp, final Iterable<Header> headers) {
        return new Record(topic, key, value, partition, offset, timestamp, headers);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final Record record = (Record) other;
        return Objects.equals(key, record.key) && Objects.equals(value, record.value)
                && headersEquals(headers, record.headers);
    }

    private boolean headersEquals(final Iterable<Header> headers1, final Iterable<Header> headers2) {
        final Iterator<Header> h1Iterator = headers1.iterator();
        final Iterator<Header> h2Iterator = headers2.iterator();
        while (h1Iterator.hasNext() && h2Iterator.hasNext()) {
            final Header header1 = h1Iterator.next();
            final Header header2 = h2Iterator.next();
            if (!Objects.equals(header1.key(), header2.key())) {
                return false;
            }
            if (!Objects.equals(header1.schema().type(), header2.schema().type())) {
                return false;
            }
            if (header1.schema().type() != Schema.Type.BYTES) {
                return false;
            }
            if (header2.schema().type() != Schema.Type.BYTES) {
                return false;
            }
            if (!Arrays.equals((byte[]) header1.value(), (byte[]) header2.value())) {
                return false;
            }
        }
        return !h1Iterator.hasNext() && !h2Iterator.hasNext();
    }

    public SinkRecord toSinkRecord() {
        return new SinkRecord(topic, partition, Schema.BYTES_SCHEMA, key.getBytes(StandardCharsets.UTF_8),
                Schema.BYTES_SCHEMA, value.getBytes(StandardCharsets.UTF_8), offset, timestamp,
                TimestampType.CREATE_TIME, headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, headers);
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(String.format("key=%s value=%s%n", key, value));
        for (final Header header : headers) {
            stringBuilder.append(String.format("%s %s%n", header.key(), Utils.bytesToHex((byte[]) header.value())));
        }
        return stringBuilder.toString();
    }
}
