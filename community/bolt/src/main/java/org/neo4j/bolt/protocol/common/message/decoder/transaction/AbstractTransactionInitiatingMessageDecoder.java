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
package org.neo4j.bolt.protocol.common.message.decoder.transaction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.decoder.MessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.util.NotificationsConfigMetadataReader;
import org.neo4j.bolt.protocol.common.message.decoder.util.TransactionInitiatingMetadataParser;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.bolt.protocol.common.message.request.transaction.AbstractTransactionInitiatingMessage;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.util.PackstreamConversions;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

public abstract class AbstractTransactionInitiatingMessageDecoder<M extends AbstractTransactionInitiatingMessage>
        implements MessageDecoder<M> {
    private static final String FIELD_ACCESS_MODE = "mode";
    private static final String FIELD_BOOKMARKS = "bookmarks";
    public static final String FIELD_TIMEOUT = "tx_timeout";
    public static final String FIELD_TYPE = "tx_type";
    private static final String FIELD_METADATA = "tx_metadata";

    protected AccessMode readAccessMode(MapValue meta) throws PackstreamReaderException {
        var accessMode = PackstreamConversions.asNullableStringValue(FIELD_ACCESS_MODE, meta.get(FIELD_ACCESS_MODE));
        if (accessMode == null) {
            return AccessMode.WRITE;
        }

        return AccessMode.byFlag(accessMode)
                .orElseThrow(() -> new IllegalStructArgumentException(
                        FIELD_ACCESS_MODE, "Expecting access mode value to be 'r' or 'w'"));
    }

    protected List<String> readBookmarks(MapValue meta) throws PackstreamReaderException {
        var listValue = PackstreamConversions.asNullableListValue(FIELD_BOOKMARKS, meta.get(FIELD_BOOKMARKS));
        if (listValue == null) {
            return List.of();
        }

        return convertBookmarks(listValue);
    }

    public static List<String> convertBookmarks(ListValue listValue) throws IllegalStructArgumentException {
        List<String> bookmarks = new ArrayList<>();
        for (var bookmark : listValue) {
            if (bookmark != Values.NO_VALUE) {
                var bookmarkString = toBookmarkString(bookmark);
                bookmarks.add(bookmarkString);
            }
        }

        return bookmarks;
    }

    private static String toBookmarkString(AnyValue bookmark) throws IllegalStructArgumentException {
        if (bookmark instanceof TextValue bookmarkString) {
            return bookmarkString.stringValue();
        }

        throw new IllegalStructArgumentException(FIELD_BOOKMARKS, "Expected list of strings");
    }

    protected NotificationsConfig readNotificationsConfig(MapValue metadata) throws IllegalStructArgumentException {
        return NotificationsConfigMetadataReader.readFromMapValue(metadata);
    }

    protected String readImpersonatedUser(MapValue meta) throws PackstreamReaderException {
        return TransactionInitiatingMetadataParser.readImpersonatedUser(meta);
    }

    protected Duration readTimeout(MapValue meta) throws PackstreamReaderException {
        var txTimeout = PackstreamConversions.asNullableLongValue(FIELD_TIMEOUT, meta.get(FIELD_TIMEOUT));

        if (!txTimeout.isPresent()) {
            return null;
        }

        return Duration.ofMillis(txTimeout.getAsLong());
    }

    protected Map<String, Object> readMetadata(MapValue meta) throws PackstreamReaderException {
        var mapValue = PackstreamConversions.asNullableMapValue(FIELD_METADATA, meta.get(FIELD_METADATA));
        if (mapValue == null) {
            return null;
        }

        var writer = new TransactionMetadataWriter();
        var txMeta = new HashMap<String, Object>(mapValue.size());
        mapValue.foreach((key, value) -> txMeta.put(key, writer.valueAsObject(value)));
        return txMeta;
    }

    private static class TransactionMetadataWriter extends BaseToObjectValueWriter<RuntimeException> {
        @Override
        protected Node newNodeEntityById(long id) {
            throw new UnsupportedOperationException("Transaction metadata should not contain nodes");
        }

        @Override
        protected Node newNodeEntityByElementId(String elementId) {
            throw new UnsupportedOperationException("Transaction metadata should not contain nodes");
        }

        @Override
        protected Relationship newRelationshipEntityById(long id) {
            throw new UnsupportedOperationException("Transaction metadata should not contain relationships");
        }

        @Override
        protected Relationship newRelationshipEntityByElementId(String elementId) {
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
