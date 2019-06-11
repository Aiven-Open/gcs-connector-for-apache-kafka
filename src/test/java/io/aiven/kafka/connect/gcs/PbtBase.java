/*
 * Aiven Kafka GCS Connector
 * Copyright (c) 2019 Aiven Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.aiven.kafka.connect.gcs;

import io.aiven.kafka.connect.gcs.config.GcsSinkConfig;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Provide;
import net.jqwik.api.RandomGenerator;
import net.jqwik.engine.properties.arbitraries.randomized.RandomGenerators;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;
import java.util.*;

abstract class PbtBase {
    private static final int MAX_TOPICS = 6;
    private static final int MAX_PARTITIONS_PER_TOPIC = 10;
    private static final int MAX_START_OFFSET = 20000;
    private static final int MAX_OFFSET_INCREMENT = 5;
    private static final int MAX_RECORDS_PER_TRY = 10000;
    private static final double PROBABILITY_OF_NEW_RECORD_BATCH = 2.0 / MAX_RECORDS_PER_TRY;

    static final String TEST_BUCKET = "test-bucket";
    static final String PREFIX = "test-dir/";

    static final int FIELD_KEY = 0;
    static final int FIELD_VALUE = 1;
    static final int FIELD_OFFSET = 2;

    @Provide
    final Arbitrary<List<List<SinkRecord>>> recordBatches() {
        final RandomGenerator<String> keyGenerator = RandomGenerators.samples(
                new String[]{"key0", "key1", "key2", "key3", null});

        // TODO make generated lists shrinkable
        return Arbitraries.randomValue(random -> {
            final List<TopicPartition> topicPartitions = new ArrayList<>();
            final Map<TopicPartition, SinkRecordBuilder> recordBuilders = new HashMap<>();

            // Randomly pick the number of topics.
            final int topicCount = random.nextInt(MAX_TOPICS - 1) + 1;
            for (int topicIdx = 0; topicIdx < topicCount; topicIdx++) {
                // Randomly pick the number of partitions for a particular topic.
                final int partitionCount = random.nextInt(MAX_PARTITIONS_PER_TOPIC - 1) + 1;
                for (int partitionIdx = 0; partitionIdx < partitionCount; partitionIdx++) {
                    final TopicPartition tp = new TopicPartition("topic" + topicIdx, partitionIdx);
                    topicPartitions.add(tp);
                    recordBuilders.put(tp, new SinkRecordBuilder(random, keyGenerator, tp.topic(), tp.partition()));
                }
            }

            // Randomly pick the number of records to generate in this try.
            final int numberOrRecords = random.nextInt(MAX_RECORDS_PER_TRY);
            final List<List<SinkRecord>> result = new ArrayList<>(numberOrRecords);
            result.add(new ArrayList<>());
            for (int i = 0; i < numberOrRecords; i++) {
                if (random.nextDouble() < PROBABILITY_OF_NEW_RECORD_BATCH) {
                    result.add(new ArrayList<>());
                }
                final TopicPartition tp = topicPartitions.get(random.nextInt(topicPartitions.size()));
                final List<SinkRecord> currentBatch = result.get(result.size() - 1);
                currentBatch.add(recordBuilders.get(tp).build());
            }
            return result;
        });
    }

    private static class SinkRecordBuilder {
        private final Random random;
        private final RandomGenerator<String> keyGenerator;
        private final String topic;
        private final int partition;

        private int offset;

        private SinkRecordBuilder(final Random random,
                                  final RandomGenerator<String> keyGenerator,
                                  final String topic,
                                  final int partition) {
            this.random = random;
            this.keyGenerator = keyGenerator;
            this.topic = topic;
            this.partition = partition;
            // In a particular topic-partition, start from a random offset.
            this.offset = random.nextInt(MAX_START_OFFSET);
        }

        final SinkRecord build() {
            final String key = keyGenerator.next(random).value();
            final String value = topic + "-" + partition;

            final SinkRecord record = new SinkRecord(
                    topic,
                    partition,
                    Schema.OPTIONAL_STRING_SCHEMA,
                    key,
                    Schema.OPTIONAL_BYTES_SCHEMA,
                    value.getBytes(StandardCharsets.UTF_8),
                    offset);
            // Imitate gaps in offsets.
            final int offsetIncrement = random.nextInt(MAX_OFFSET_INCREMENT - 1) + 1;
            offset += offsetIncrement;
            return record;
        }
    }

    Map<String, String> basicTaskProps() {
        final Map<String, String> taskProps = new HashMap<>();
        taskProps.put(GcsSinkConfig.GCS_BUCKET_NAME_CONFIG, TEST_BUCKET);
        taskProps.put(GcsSinkConfig.FILE_NAME_PREFIX_CONFIG, PREFIX);
        taskProps.put(GcsSinkConfig.FILE_NAME_TEMPLATE_CONFIG, "{{topic}}-{{partition}}-{{start_offset}}");
        taskProps.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,offset");
        return taskProps;
    }
}
