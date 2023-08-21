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
package org.neo4j.bolt.protocol.common.message.decoder.util;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;

class TransactionInitiatingMetadataParserTest {

    @Test
    void shouldReadDatabaseName() throws PackstreamReaderException {
        var builder = new MapValueBuilder();
        builder.add("foo", Values.stringValue("bar"));
        builder.add("db", Values.stringValue("neo5j"));
        builder.add("imp_user", Values.stringValue("foo"));
        var meta = builder.build();

        var result = TransactionInitiatingMetadataParser.readDatabaseName(meta);

        Assertions.assertThat(result).isEqualTo("neo5j");
    }

    @Test
    void readDatabaseNameShouldPermitOmittedProperty() throws PackstreamReaderException {
        var builder = new MapValueBuilder();
        builder.add("foo", Values.stringValue("bar"));
        builder.add("imp_user", Values.stringValue("foo"));
        var meta = builder.build();

        var result = TransactionInitiatingMetadataParser.readDatabaseName(meta);

        Assertions.assertThat(result).isNull();
    }

    @Test
    void readDatabaseNameShouldInterpretEmptyPropertyAsDefault() throws PackstreamReaderException {
        var builder = new MapValueBuilder();
        builder.add("foo", Values.stringValue("bar"));
        builder.add("db", Values.stringValue(""));
        builder.add("imp_user", Values.stringValue("foo"));
        var meta = builder.build();

        var result = TransactionInitiatingMetadataParser.readDatabaseName(meta);

        Assertions.assertThat(result).isNull();
    }

    @Test
    void readDatabaseNameShouldFailWithIllegalStructArgumentWhenInvalidValueIsGiven() throws PackstreamReaderException {
        var builder = new MapValueBuilder();
        builder.add("foo", Values.stringValue("bar"));
        builder.add("db", Values.longValue(42));
        builder.add("imp_user", Values.stringValue("foo"));
        var meta = builder.build();

        Assertions.assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> TransactionInitiatingMetadataParser.readDatabaseName(meta))
                .withMessage("Illegal value for field \"db\": Expected string")
                .withNoCause();
    }

    @Test
    void shouldReadImpersonatedUser() throws PackstreamReaderException {
        var builder = new MapValueBuilder();
        builder.add("foo", Values.stringValue("bar"));
        builder.add("db", Values.stringValue("neo5j"));
        builder.add("imp_user", Values.stringValue("foo"));
        var meta = builder.build();

        var result = TransactionInitiatingMetadataParser.readImpersonatedUser(meta);

        Assertions.assertThat(result).isNotNull().isEqualTo("foo");
    }

    @Test
    void readImpersonatedUserShouldPermitOmittedProperty() throws PackstreamReaderException {
        var builder = new MapValueBuilder();
        builder.add("foo", Values.stringValue("bar"));
        builder.add("db", Values.stringValue("neo5j"));
        var meta = builder.build();

        var result = TransactionInitiatingMetadataParser.readImpersonatedUser(meta);

        Assertions.assertThat(result).isNull();
    }

    @Test
    void readImpersonatedUserShouldFailWithIllegalStructArgumentWhenInvalidValueIsGiven() {
        var builder = new MapValueBuilder();
        builder.add("foo", Values.stringValue("bar"));
        builder.add("db", Values.longValue(42));
        builder.add("imp_user", Values.longValue(42));
        var meta = builder.build();

        Assertions.assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> TransactionInitiatingMetadataParser.readImpersonatedUser(meta))
                .withMessage("Illegal value for field \"imp_user\": Expected string")
                .withNoCause();
    }
}
