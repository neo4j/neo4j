/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.v40.messaging.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

class MessageMetadataParserV40Test {

    @Test
    void shouldParseTransactionTimeout() throws PackstreamReaderException {
        var meta = new MapValueBuilder();
        meta.add("tx_timeout", Values.longValue(42));

        var txTimeout = MessageMetadataParserV40.parseTransactionTimeout(meta.build());

        assertThat(txTimeout).isNotNull().isEqualTo(Duration.ofMillis(42));
    }

    @Test
    void shouldHandleMissingTransactionTimeout() throws PackstreamReaderException {
        var txTimeout = MessageMetadataParserV40.parseTransactionTimeout(MapValue.EMPTY);

        assertThat(txTimeout).isNull();
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidTransactionTimeoutIsGiven() {
        var meta = new MapValueBuilder();
        meta.add("tx_timeout", Values.stringValue("the_answer"));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> MessageMetadataParserV40.parseTransactionTimeout(meta.build()))
                .withMessage(
                        "Illegal value for field \"tx_timeout\": Expecting transaction timeout value to be a Long value, but got: String(\"the_answer\")")
                .withNoCause();
    }

    @Test
    void shouldParseAccessMode() throws PackstreamReaderException {
        var meta = new MapValueBuilder();
        meta.add("mode", Values.stringValue("r")); // w is assumed as a fallback

        var mode = MessageMetadataParserV40.parseAccessMode(meta.build());

        assertThat(mode).isNotNull().isEqualTo(AccessMode.READ);
    }

    @Test
    void shouldFallBackToWriteWhenNoAccessModeIsGiven() throws PackstreamReaderException {
        var mode = MessageMetadataParserV40.parseAccessMode(MapValue.EMPTY);

        assertThat(mode).isNotNull().isEqualTo(AccessMode.WRITE);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidAccessModeIsGiven() {
        var meta = new MapValueBuilder();
        meta.add("mode", Values.longValue(42));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> MessageMetadataParserV40.parseAccessMode(meta.build()))
                .withMessage(
                        "Illegal value for field \"mode\": Expecting transaction access mode value to be a String value, but got: Long(42)")
                .withNoCause();
    }

    @Test
    void shouldParseTransactionMetadata() throws PackstreamReaderException {
        var txMeta = new MapValueBuilder();
        txMeta.add("the_answer", Values.longValue(42));
        txMeta.add("something_else", Values.stringValue("Pizza!"));

        var meta = new MapValueBuilder();
        meta.add("tx_metadata", txMeta.build());

        var actual = MessageMetadataParserV40.parseTransactionMetadata(meta.build());

        assertThat(actual).hasSize(2).containsEntry("the_answer", 42L).containsEntry("something_else", "Pizza!");
    }

    @Test
    void shouldHandleMissingTransactionMetadata() throws PackstreamReaderException {
        var actual = MessageMetadataParserV40.parseTransactionMetadata(MapValue.EMPTY);

        assertThat(actual).isNull();
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidTransactionMetadataIsGiven() {
        var meta = new MapValueBuilder();
        meta.add("tx_metadata", Values.longValue(42));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> MessageMetadataParserV40.parseTransactionMetadata(meta.build()))
                .withMessage(
                        "Illegal value for field \"tx_metadata\": Expecting transaction metadata value to be a Map value, but got: Long(42)")
                .withNoCause();
    }

    @Test
    void shouldParseDatabaseName() throws PackstreamReaderException {
        var meta = new MapValueBuilder();
        meta.add("db", Values.stringValue("neo4j"));

        var db = MessageMetadataParserV40.parseDatabaseName(meta.build());

        assertThat(db).isNotNull().isEqualTo("neo4j");
    }

    @Test
    void shouldFallbackToAbsentDatabaseMarkerWhenDatabaseNameIsOmitted() throws PackstreamReaderException {
        var db = MessageMetadataParserV40.parseDatabaseName(MapValue.EMPTY);

        assertThat(db).isNotNull().isEqualTo(MessageMetadataParserV40.ABSENT_DB_NAME);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidDatabaseNameIsGiven() {
        var meta = new MapValueBuilder();
        meta.add("db", Values.longValue(42));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> MessageMetadataParserV40.parseDatabaseName(meta.build()))
                .withMessage(
                        "Illegal value for field \"db\": Expecting database name value to be a String value, but got: Long(42)")
                .withNoCause();
    }

    @Test
    void shouldParseStreamLimit() throws PackstreamReaderException {
        var meta = new MapValueBuilder();
        meta.add("n", Values.longValue(42));

        var limit = MessageMetadataParserV40.parseStreamLimit(meta.build());

        assertThat(limit).isEqualTo(42);
    }

    @Test
    void shouldParseUnlimitedStreamLimitMarker() throws PackstreamReaderException {
        var meta = new MapValueBuilder();
        meta.add("n", Values.longValue(-1));

        var limit = MessageMetadataParserV40.parseStreamLimit(meta.build());

        assertThat(limit).isEqualTo(-1);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenStreamLimitIsOmitted() {
        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> MessageMetadataParserV40.parseStreamLimit(MapValue.EMPTY))
                .withMessage("Illegal value for field \"n\": Expecting size to be a Long value, but got: NO_VALUE")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidStreamLimitIsGiven() {
        var meta = new MapValueBuilder();
        meta.add("n", Values.stringValue("foo"));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> MessageMetadataParserV40.parseStreamLimit(meta.build()))
                .withMessage(
                        "Illegal value for field \"n\": Expecting size to be a Long value, but got: String(\"foo\")")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenNegativeStreamLimitIsGiven() {
        var meta = new MapValueBuilder();
        meta.add("n", Values.longValue(-2));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> MessageMetadataParserV40.parseStreamLimit(meta.build()))
                .withMessage("Illegal value for field \"n\": Expecting size to be at least 1, but got: -2")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenZeroSizeLimitIsGiven() {
        var meta = new MapValueBuilder();
        meta.add("n", Values.longValue(0));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> MessageMetadataParserV40.parseStreamLimit(meta.build()))
                .withMessage("Illegal value for field \"n\": Expecting size to be at least 1, but got: 0")
                .withNoCause();
    }

    @Test
    void shouldParseStatementId() throws PackstreamReaderException {
        var meta = new MapValueBuilder();
        meta.add("qid", Values.longValue(42));

        var statementId = MessageMetadataParserV40.parseStatementId(meta.build());

        assertThat(statementId).isEqualTo(42);
    }

    @Test
    void shouldFallBackToAbsentMarkerWhenStatementIdIsOmitted() throws PackstreamReaderException {
        var statementId = MessageMetadataParserV40.parseStatementId(MapValue.EMPTY);

        assertThat(statementId).isEqualTo(-1);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidStatementIdIsGiven() {
        var meta = new MapValueBuilder();
        meta.add("qid", Values.stringValue("foo"));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> MessageMetadataParserV40.parseStatementId(meta.build()))
                .withMessage(
                        "Illegal value for field \"qid\": Expecting statement id to be a Long value, but got: String(\"foo\")")
                .withNoCause();
    }
}
