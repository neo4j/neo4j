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

package org.neo4j.bolt.protocol.v51.message.decoder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.bolt.protocol.v51.message.request.HelloMessage;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.virtual.MapValue;

public class HelloMessageDecoder implements StructReader<Connection, HelloMessage> {
    private static final HelloMessageDecoder INSTANCE = new HelloMessageDecoder();

    public static final String ROUTING_KEY = "routing";

    protected HelloMessageDecoder() {}

    public static HelloMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return HelloMessage.SIGNATURE;
    }

    @Override
    public HelloMessage read(Connection ctx, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        if (header.length() != 1) {
            throw new IllegalStructSizeException(1, header.length());
        }

        var valueReader = ctx.valueReader(buffer);
        Map<String, Object> meta =
                this.readMetadataMap(valueReader, buffer.getTarget().readableBytes());

        this.validateMeta(meta);

        var routingContext = parseRoutingContext(meta);

        return new HelloMessage(meta, routingContext);
    }

    protected void validateMeta(Map<String, Object> meta) throws PackstreamReaderException {
        var userAgent = meta.get("user_agent");
        if (userAgent == null) {
            throw new IllegalStructArgumentException("user_agent", "Expected \"user_agent\" to be non-null");
        }
        if (!(userAgent instanceof String)) {
            throw new IllegalStructArgumentException("user_agent", "Expected value to be a string");
        }
    }

    @SuppressWarnings("unchecked")
    private static RoutingContext parseRoutingContext(Map<String, Object> meta) throws PackstreamReaderException {
        var property = meta.getOrDefault(ROUTING_KEY, null);

        if (property == null) {
            return new RoutingContext(false, Collections.emptyMap());
        }

        // TODO: Refactor this streaming logic to rely on Packstream types instead of manual validation
        if (!(property instanceof Map<?, ?>)) {
            throw new IllegalStructArgumentException(
                    "routing", "Expected map but got " + property.getClass().getSimpleName());
        }

        var routingObjectMap = (Map<String, Object>) meta.getOrDefault(ROUTING_KEY, null);
        var routingStringMap = new HashMap<String, String>();
        for (var entry : routingObjectMap.entrySet()) {
            if (entry.getValue() instanceof String stringValue) {
                routingStringMap.put(entry.getKey(), stringValue);
                continue;
            }

            // TODO: Technically inconsistent - Base reports errors on meta
            throw new IllegalStructArgumentException("routing", "Must be a map with string keys and string values.");
        }

        return new RoutingContext(true, Map.copyOf(routingStringMap));
    }

    protected Map<String, Object> readMetadataMap(PackstreamValueReader<Connection> reader, int limit)
            throws PackstreamReaderException {
        MapValue metaDataMapValue;
        try {
            metaDataMapValue = reader.readPrimitiveMap(limit);
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("extra", ex);
        }

        var writer = new HelloMessageValueWriter();
        var metaDataMap = new HashMap<String, Object>(metaDataMapValue.size());
        metaDataMapValue.foreach((key, value) -> {
            value.writeTo(writer);
            metaDataMap.put(key, writer.value());
        });

        return metaDataMap;
    }

    // This is used basically to convert from StringValue to string and ensure that no illegal structures
    // are present.
    private static class HelloMessageValueWriter extends BaseToObjectValueWriter<RuntimeException> {
        @Override
        protected Node newNodeEntityById(long id) {
            throw new UnsupportedOperationException("Hello Message should not contain nodes");
        }

        @Override
        protected Node newNodeEntityByElementId(String elementId) {
            throw new UnsupportedOperationException("Hello Message should not contain nodes");
        }

        @Override
        protected Relationship newRelationshipEntityById(long id) {
            throw new UnsupportedOperationException("Hello Message should not contain relationships");
        }

        @Override
        protected Relationship newRelationshipEntityByElementId(String elementId) {
            throw new UnsupportedOperationException("Hello Message should not contain relationships");
        }

        @Override
        protected Point newPoint(CoordinateReferenceSystem crs, double[] coordinate) {
            throw new UnsupportedOperationException("Hello Message should not contain relationships");
        }

        Object valueAsObject(AnyValue value) {
            value.writeTo(this);
            return value();
        }
    }
}
