/*
 * Aiven Kafka GCS Connector
 * Copyright (c) 2019 Aiven Oy
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GcsSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(GcsSinkConnector.class);

    private Map<String, String> configProps;
    private GcsSinkConfig config;

    // required by Connect
    public GcsSinkConnector() {
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props cannot be null");

        this.configProps = Collections.unmodifiableMap(props);
        this.config = new GcsSinkConfig(props);
        log.info("Starting connector {}", config.getConnectorName());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GcsSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            final Map<String, String> config = new HashMap<>(configProps);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do.
        log.info("Stopping connector {}", config.getConnectorName());
    }

    @Override
    public ConfigDef config() {
        return GcsSinkConfig.configDef();
    }
}
