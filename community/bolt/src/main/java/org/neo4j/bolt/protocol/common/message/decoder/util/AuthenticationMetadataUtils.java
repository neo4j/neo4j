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

import static org.neo4j.values.storable.Values.NO_VALUE;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.UTF8StringValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.MapValue;

/**
 * Provides methods to extract message fields related within authentication messages.
 */
public final class AuthenticationMetadataUtils {

    private AuthenticationMetadataUtils() {}

    public static Map<String, Object> extractAuthToken(List<String> ignoredFields, Map<String, Object> input) {
        return input.entrySet().stream()
                .filter(e -> !ignoredFields.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<String, Object> convertExtraMap(PackstreamValueReader<Connection> reader, int limit)
            throws PackstreamReaderException {
        MapValue metadataMapValue;
        try {
            metadataMapValue = reader.readPrimitiveMap(limit);
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("extra", ex);
        }

        var writer = new AuthTokenValueWriter();
        var metadataMap = new HashMap<String, Object>(metadataMapValue.size());
        metadataMapValue.foreach((key, value) -> {
            if (!AuthToken.containsSensitiveInformation(key)) {
                value.writeTo(writer);
                metadataMap.put(key, writer.value());
            } else {
                metadataMap.put(key, sensitiveValueAsObject(value));
            }
        });

        return metadataMap;
    }

    private static Object sensitiveValueAsObject(AnyValue value) {
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
        protected Node newNodeEntityByElementId(String elementId) {
            throw new UnsupportedOperationException("Authentication tokens should not contain nodes");
        }

        @Override
        protected Relationship newRelationshipEntityById(long id) {
            throw new UnsupportedOperationException("Authentication tokens should not contain relationships");
        }

        @Override
        protected Relationship newRelationshipEntityByElementId(String elementId) {
            throw new UnsupportedOperationException("Authentication tokens should not contain relationships");
        }

        @Override
        protected Point newPoint(CoordinateReferenceSystem crs, double[] coordinate) {
            throw new UnsupportedOperationException("Authentication tokens should not contain relationships");
        }
    }
}
