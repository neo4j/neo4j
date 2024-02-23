/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cli;

import static java.lang.String.format;
import static org.neo4j.configuration.ToolingMemoryCalculations.NOTIFY_SYS_ERR;
import static org.neo4j.configuration.helpers.DatabaseNameValidator.validateInternalDatabaseName;

import java.time.Duration;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.ToolingMemoryCalculations;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.TypeConversionException;

public interface Converters {

    class ByteUnitConverter implements ITypeConverter<Long> {
        @Override
        public Long convert(String value) {
            try {
                return ByteUnit.parse(value);
            } catch (Exception e) {
                throw new TypeConversionException(format("cannot convert '%s' to byte units (%s)", value, e));
            }
        }
    }

    class DatabaseNameConverter implements ITypeConverter<NormalizedDatabaseName> {
        @Override
        public NormalizedDatabaseName convert(String name) {
            try {
                var databaseName = new NormalizedDatabaseName(name);
                validateInternalDatabaseName(databaseName);
                return databaseName;
            } catch (Exception e) {
                throw new TypeConversionException(format("Invalid database name '%s'. (%s)", name, e));
            }
        }
    }

    class DatabaseNamePatternConverter implements ITypeConverter<DatabaseNamePattern> {

        @Override
        public DatabaseNamePattern convert(String name) {
            try {
                return new DatabaseNamePattern(name);
            } catch (Exception ex) {
                throw new TypeConversionException(format("Invalid database name '%s'. (%s)", name, ex));
            }
        }
    }

    class MaxOffHeapMemoryConverter implements ITypeConverter<Long> {
        @Override
        public Long convert(String value) throws Exception {
            return new ToolingMemoryCalculations(NOTIFY_SYS_ERR).calculateMaxAvailableOffHeapMemory(value);
        }
    }

    class DurationConverter implements ITypeConverter<Duration> {

        @Override
        public Duration convert(String value) throws Exception {
            try {
                return SettingValueParsers.DURATION.parse(value);
            } catch (Exception ex) {
                throw new TypeConversionException(
                        String.format("'%s' is not a valid duration. %s", value, ex.getMessage()));
            }
        }
    }
}
