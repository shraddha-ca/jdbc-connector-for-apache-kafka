/*
 * Copyright 2020 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2018 Confluent Inc.
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

package io.aiven.connect.jdbc.dialect;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;

/**
 * A {@link DatabaseDialect} for Redshift.
 */
public class RedshiftDatabaseDialect extends PostgreSqlDatabaseDialect {

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public RedshiftDatabaseDialect(final JdbcConfig config) {
        super(config);
    }

    @Override
    protected String getSqlType(final SinkRecordField field) {
        final String sqlType = getSqlTypeFromSchema(field.schema());
        return sqlType != null ? sqlType : super.getSqlType(field);
    }

    private String getSqlTypeFromSchema(final Schema schema) {
        if (schema.name() != null) {
            switch (schema.name()) {
                case Decimal.LOGICAL_NAME:
                    return "DECIMAL";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case Time.LOGICAL_NAME:
                    return "TIME";
                case Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP";
                default:
                    // fall through to normal types
            }
        }
        switch (schema.type()) {
            case INT8:
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "REAL";
            case FLOAT64:
                return "DOUBLE PRECISION";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR(65535)";
            case ARRAY:
                return "SUPER";
            default:
                return null;
        }
    }
}
