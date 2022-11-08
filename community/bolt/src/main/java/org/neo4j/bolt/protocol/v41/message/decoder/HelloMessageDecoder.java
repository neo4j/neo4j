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
package org.neo4j.bolt.protocol.v41.message.decoder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.v40.messaging.request.HelloMessage;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public final class HelloMessageDecoder extends org.neo4j.bolt.protocol.v40.messaging.decoder.HelloMessageDecoder {
    private static final HelloMessageDecoder INSTANCE = new HelloMessageDecoder();

    public static final String ROUTING_KEY = "routing";

    private HelloMessageDecoder() {}

    public static HelloMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public HelloMessage read(Connection ctx, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        if (header.length() != 1) {
            throw new IllegalStructSizeException(1, header.length());
        }

        var valueReader = ctx.valueReader(buffer);
        Map<String, Object> meta =
                this.readMetaDataMap(valueReader, buffer.getTarget().readableBytes());

        this.validateMeta(meta);

        var routingContext = parseRoutingContext(meta);
        var authToken = extractAuthToken(meta);

        return newHelloMessage(meta, routingContext, authToken);
    }

    private HelloMessage newHelloMessage(
            Map<String, Object> meta, RoutingContext routingContext, Map<String, Object> authToken) {
        return new org.neo4j.bolt.protocol.v41.message.request.HelloMessage(
                meta, routingContext, extractAuthToken(meta));
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

    private Map<String, Object> extractAuthToken(Map<String, Object> meta) {
        // The authToken is currently nothing more than the Hello metadata minus the routing context.
        return meta.entrySet().stream()
                .filter(e -> !ROUTING_KEY.equals(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
