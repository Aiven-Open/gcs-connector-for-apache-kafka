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

import com.google.common.base.Objects;

public class OutputField {
    private OutputFieldType fieldType;
    private OutputFieldEncodingType encodingType;

    public OutputField(final OutputFieldType fieldType, final OutputFieldEncodingType encodingType) {
        this.fieldType = fieldType;
        this.encodingType = encodingType;
    }

    public OutputFieldType getFieldType() {
        return fieldType;
    }

    public OutputFieldEncodingType getEncodingType() {
        return encodingType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldType, encodingType);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof OutputField)) {
            return false;
        }

        final OutputField that = (OutputField) obj;

        return Objects.equal(this.fieldType, that.fieldType)
                && Objects.equal(this.encodingType, that.encodingType);
    }
}
