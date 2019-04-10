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

package io.aiven.kafka.connect.gcs.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.*;

/**
 * A {@link ConfigDef.Recommender} that always supports only
 * the predefined set of values. {@link #visible(String, Map)} is always {@code true}.
 */
class FixedSetRecommender implements ConfigDef.Recommender {

    private final List<Object> supportedValues;

    private FixedSetRecommender(final Collection<?> supportedValues) {
        Objects.requireNonNull(supportedValues);
        this.supportedValues = new ArrayList<>(supportedValues);
    }

    @Override
    public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
        return Collections.unmodifiableList(supportedValues);
    }

    @Override
    public boolean visible(final String name, final Map<String, Object> parsedConfig) {
        return true;
    }

    static FixedSetRecommender ofSupportedValues(final Collection<?> supportedValues) {
        Objects.requireNonNull(supportedValues);
        return new FixedSetRecommender(supportedValues);
    }
}
