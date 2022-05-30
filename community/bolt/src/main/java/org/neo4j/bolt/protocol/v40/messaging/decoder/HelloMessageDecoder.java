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
package org.neo4j.bolt.protocol.v40.messaging.decoder;

import static org.neo4j.values.storable.Values.NO_VALUE;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.bolt.protocol.v40.messaging.request.HelloMessage;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValues;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.UTF8StringValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.MapValue;

public class HelloMessageDecoder implements StructReader<HelloMessage> {
    private static final HelloMessageDecoder INSTANCE = new HelloMessageDecoder();

    protected HelloMessageDecoder() {}

    public static HelloMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return HelloMessage.SIGNATURE;
    }

    @Override
    public HelloMessage read(PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        if (header.length() != 1) {
            throw new IllegalStructSizeException(1, header.length());
        }

        var meta = readMetaDataMap(buffer);

        var userAgent = meta.get("user_agent");
        if (userAgent == null) {
            throw new IllegalStructArgumentException("user_agent", "Expected \"user_agent\" to be non-null");
        }
        if (!(userAgent instanceof String)) {
            throw new IllegalStructArgumentException("user_agent", "Expected value to be a string");
        }

        return new HelloMessage(meta);
    }

    protected static Map<String, Object> readMetaDataMap(PackstreamBuf buf) throws PackstreamReaderException {
        MapValue metaDataMapValue;
        try {
            metaDataMapValue =
                    PackstreamValues.readPrimitiveMap(buf, buf.getTarget().readableBytes());
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("extra", ex);
        }

        var writer = new AuthTokenValueWriter();
        var metaDataMap = new HashMap<String, Object>(metaDataMapValue.size());
        metaDataMapValue.foreach((key, value) -> {
            if (!AuthToken.containsSensitiveInformation(key)) {
                value.writeTo(writer);
                metaDataMap.put(key, writer.value());
            } else {
                metaDataMap.put(key, sensitiveValueAsObject(value));
            }
        });

        return metaDataMap;
    }

    protected static Object sensitiveValueAsObject(AnyValue value) {
        if (value instanceof UTF8StringValue stringValue) {
            return stringValue.bytes();
        }
        if (value instanceof StringValue stringValue) {
            if (stringValue.isEmpty()) {
                return ArrayUtils.EMPTY_BYTE_ARRAY;
            }

            return stringValue.stringValue().getBytes(StandardCharsets.UTF_8);
        }

        if (value == NO_VALUE) {
            return null;
        }

        return ((Value) value).asObjectCopy();
    }

    private static class AuthTokenValueWriter extends BaseToObjectValueWriter<RuntimeException> {
        @Override
        protected Node newNodeEntityById(long id) {
            throw new UnsupportedOperationException("Authentication tokens should not contain nodes");
        }

        @Override
        protected Relationship newRelationshipEntityById(long id) {
            throw new UnsupportedOperationException("Authentication tokens should not contain relationships");
        }

        @Override
        protected Point newPoint(CoordinateReferenceSystem crs, double[] coordinate) {
            throw new UnsupportedOperationException("Authentication tokens should not contain relationships");
        }

        Object valueAsObject(AnyValue value) {
            value.writeTo(this);
            return value();
        }
    }
}
