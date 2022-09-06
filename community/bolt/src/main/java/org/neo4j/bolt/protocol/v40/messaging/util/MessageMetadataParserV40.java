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

import static java.lang.String.format;
import static org.neo4j.bolt.protocol.v40.fsm.InTransactionState.QUERY_ID_KEY;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.bookmark.BookmarkParser;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.transaction.statement.metadata.StatementMetadata;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

public final class MessageMetadataParserV40 {
    private static final String BOOKMARKS_KEY = "bookmarks";
    private static final String TX_TIMEOUT_KEY = "tx_timeout";
    private static final String TX_META_DATA_KEY = "tx_metadata";
    private static final String ACCESS_MODE_KEY = "mode";

    public static final String DB_NAME_KEY = "db";
    public static final String ABSENT_DB_NAME = "";

    private static final String STREAM_LIMIT_KEY = "n";
    private static final long STREAM_LIMIT_MINIMAL = 1;
    public static final long STREAM_LIMIT_UNLIMITED = -1;

    private MessageMetadataParserV40() {}

    public static List<Bookmark> parseBookmarks(BookmarkParser parser, MapValue meta) throws PackstreamReaderException {
        var anyValue = meta.get(BOOKMARKS_KEY);
        if (anyValue == Values.NO_VALUE) {
            return List.of();
        }

        if (anyValue instanceof ListValue listValue) {
            return parser.parseBookmarks((ListValue) anyValue);
        }

        throw new IllegalStructArgumentException(
                BOOKMARKS_KEY, "Expecting bookmarks value to be a List of strings, but got: " + anyValue);
    }

    public static Duration parseTransactionTimeout(MapValue meta) throws PackstreamReaderException {
        var anyValue = meta.get(TX_TIMEOUT_KEY);
        if (anyValue == Values.NO_VALUE) {
            return null;
        }

        if (anyValue instanceof LongValue longValue) {
            return Duration.ofMillis(longValue.longValue());
        }

        throw new IllegalStructArgumentException(
                TX_TIMEOUT_KEY, "Expecting transaction timeout value to be a Long value, but got: " + anyValue);
    }

    public static AccessMode parseAccessMode(MapValue meta) throws PackstreamReaderException {
        var anyValue = meta.get(ACCESS_MODE_KEY);
        if (anyValue == Values.NO_VALUE) {
            return AccessMode.WRITE;
        }

        if (anyValue instanceof StringValue stringValue) {
            return AccessMode.byFlag(stringValue.stringValue())
                    .orElseThrow(() -> new IllegalStructArgumentException(
                            "mode",
                            "Expecting access mode value to be 'r' or 'w', but got: " + stringValue.stringValue()));
        }

        throw new IllegalStructArgumentException(
                ACCESS_MODE_KEY, "Expecting transaction access mode value to be a String value, but got: " + anyValue);
    }

    public static Map<String, Object> parseTransactionMetadata(MapValue meta) throws PackstreamReaderException {
        var anyValue = meta.get(TX_META_DATA_KEY);
        if (anyValue == Values.NO_VALUE) {
            return null;
        }

        if (anyValue instanceof MapValue mapValue) {
            var writer = new TransactionMetadataWriter();

            Map<String, Object> txMeta = new HashMap<>(mapValue.size());
            mapValue.foreach((key, value) -> txMeta.put(key, writer.valueAsObject(value)));
            return txMeta;
        }

        throw new IllegalStructArgumentException(
                TX_META_DATA_KEY, "Expecting transaction metadata value to be a Map value, but got: " + anyValue);
    }

    /**
     * Empty or null value indicates the default database is selected
     */
    public static String parseDatabaseName(MapValue meta) throws PackstreamReaderException {
        var anyValue = meta.get(DB_NAME_KEY);
        if (anyValue == Values.NO_VALUE) {
            return ABSENT_DB_NAME;
        }

        if (anyValue instanceof StringValue) {
            return ((StringValue) anyValue).stringValue();
        }

        throw new IllegalStructArgumentException(
                "db", "Expecting database name value to be a String value, but got: " + anyValue);
    }

    public static long parseStreamLimit(MapValue meta) throws PackstreamReaderException {
        var anyValue = meta.get(STREAM_LIMIT_KEY);

        if (anyValue instanceof LongValue longValue) {
            long size = longValue.longValue();

            if (size != STREAM_LIMIT_UNLIMITED && size < STREAM_LIMIT_MINIMAL) {
                throw new IllegalStructArgumentException(
                        "n", format("Expecting size to be at least %s, but got: %s", STREAM_LIMIT_MINIMAL, size));
            }

            return size;
        }

        throw new IllegalStructArgumentException(
                STREAM_LIMIT_KEY, format("Expecting size to be a Long value, but got: %s", anyValue));
    }

    public static int parseStatementId(MapValue meta) throws PackstreamReaderException {
        var anyValue = meta.get(QUERY_ID_KEY);

        if (anyValue == Values.NO_VALUE) {
            return StatementMetadata.ABSENT_QUERY_ID;
        }

        if (anyValue instanceof LongValue longValue) {
            return Math.toIntExact(longValue.longValue());
        }

        throw new IllegalStructArgumentException(
                QUERY_ID_KEY, format("Expecting statement id to be a Long value, but got: %s", anyValue));
    }

    private static class TransactionMetadataWriter extends BaseToObjectValueWriter<RuntimeException> {
        @Override
        protected Node newNodeEntityById(long id) {
            throw new UnsupportedOperationException("Transaction metadata should not contain nodes");
        }

        @Override
        protected Relationship newRelationshipEntityById(long id) {
            throw new UnsupportedOperationException("Transaction metadata should not contain relationships");
        }

        @Override
        protected Point newPoint(CoordinateReferenceSystem crs, double[] coordinate) {
            return Values.pointValue(crs, coordinate);
        }

        Object valueAsObject(AnyValue value) {
            value.writeTo(this);
            return value();
        }
    }
}
