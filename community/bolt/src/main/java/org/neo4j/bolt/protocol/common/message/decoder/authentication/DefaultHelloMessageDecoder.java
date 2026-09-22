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
package org.neo4j.bolt.protocol.common.message.decoder.authentication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.message.decoder.MessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.util.AuthenticationMetadataUtils;
import org.neo4j.bolt.protocol.common.message.decoder.util.NotificationsConfigMetadataReader;
import org.neo4j.boltmessages.notifications.NotificationsConfig;
import org.neo4j.boltmessages.request.authentication.HelloMessage;
import org.neo4j.boltmessages.request.connection.RoutingContext;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.FatalIllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.util.PackstreamConditions;
import org.neo4j.packstream.util.PackstreamConversions;

public class DefaultHelloMessageDecoder implements MessageDecoder<HelloMessage> {

    protected static final String FIELD_FEATURES = "patch_bolt";
    protected static final String FIELD_USER_AGENT = "user_agent";
    protected static final String FIELD_BOLT_AGENT = "bolt_agent";
    protected static final String FIELD_ROUTING = "routing";
    protected static final String FILED_NOTIFICATIONS_MIN_SEVERITY = "notifications_minimum_severity";
    protected static final String FILED_NOTIFICATIONS_DISABLED_CATEGORIES = "notifications_disabled_categories";
    private static final DefaultHelloMessageDecoder INSTANCE = new DefaultHelloMessageDecoder();

    protected DefaultHelloMessageDecoder() {}

    public static DefaultHelloMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return HelloMessage.SIGNATURE;
    }

    @Override
    public HelloMessage read(Connection ctx, PackstreamBuf buffer, StructHeader header)
            throws PackstreamReaderException {
        PackstreamConditions.requireLength(header, 1);

        var reader = ctx.valueReader(buffer);
        var meta =
                AuthenticationMetadataUtils.convertExtraMap(reader, buffer.raw().readableBytes());

        var userAgent = this.readUserAgent(meta);
        var features = this.readFeatures(meta);
        var routingContext = this.readRoutingContext(meta);
        var authToken = this.readAuthToken(meta);
        var notificationsConfig = this.readNotificationsConfig(meta);
        var boltAgent = this.readBoltAgent(meta);

        return new HelloMessage(userAgent, features, routingContext, authToken, notificationsConfig, boltAgent);
    }

    protected String readUserAgent(Map<String, Object> meta) throws PackstreamReaderException {
        return PackstreamConversions.asString(FIELD_USER_AGENT, meta.get(FIELD_USER_AGENT));
    }

    protected List<String> readFeatures(Map<String, Object> meta) {
        if (meta.get(FIELD_FEATURES) instanceof List<?> listValue) {
            var features = new ArrayList<String>();
            for (var it : listValue) {
                if (it instanceof String featureId && Feature.findFeatureById(featureId) != null) {
                    features.add(featureId);
                }
            }
            return Collections.unmodifiableList(features);
        }

        // since this is an optional protocol feature which was introduced after the original spec was written, we're
        // not going to strictly validate the list or its contents
        return Collections.emptyList();
    }

    protected RoutingContext readRoutingContext(Map<String, Object> meta) throws PackstreamReaderException {
        var property = meta.getOrDefault(FIELD_ROUTING, null);
        if (property == null) {
            return new RoutingContext(false, Collections.emptyMap());
        }

        var routingStringMap = convertToStringMap(property, FIELD_ROUTING);
        if (routingStringMap != null) return new RoutingContext(true, Map.copyOf(routingStringMap));

        // DRI-012
        throw FatalIllegalStructArgumentException.wrongTypeForFieldName(
                FIELD_ROUTING,
                property.toString(),
                List.of("MAP"),
                property.getClass().getSimpleName(),
                "Expected map but got " + property.getClass().getSimpleName());
    }

    protected Map<String, Object> readAuthToken(Map<String, Object> meta) {
        // as of protocol version 5.1, authentication is no longer part of HELLO thus this method
        // always returns null instead
        return null;
    }

    protected NotificationsConfig readNotificationsConfig(Map<String, Object> meta) throws PackstreamReaderException {
        return NotificationsConfigMetadataReader.readFromMap(meta);
    }

    protected Map<String, String> readBoltAgent(Map<String, Object> meta) throws PackstreamReaderException {
        var boltAgent = meta.get(FIELD_BOLT_AGENT);
        var map = convertToStringMap(boltAgent, FIELD_BOLT_AGENT);
        if (map == null) {
            // DRI-014
            throw IllegalStructArgumentException.wrongTypeForFieldName(
                    FIELD_BOLT_AGENT,
                    String.valueOf(boltAgent),
                    List.of("MAP<STRING, STRING>"),
                    boltAgent == null ? "null" : boltAgent.getClass().getSimpleName(),
                    "Must be a map with string keys and string values.");
        }
        if (!map.containsKey("product")) {
            throw IllegalStructArgumentException.expectedMapToHaveKey("product", FIELD_BOLT_AGENT);
        }
        return map;
    }

    private static Map<String, String> convertToStringMap(Object property, String field)
            throws IllegalStructArgumentException {
        if (!(property instanceof Map<?, ?> propertyMap)) {
            return null;
        }

        var validatedMap = new HashMap<String, String>();
        for (var entry : propertyMap.entrySet()) {
            if (!(entry.getKey() instanceof String key && entry.getValue() instanceof String value)) {
                // DRI-014
                throw FatalIllegalStructArgumentException.wrongTypeForFieldName(
                        field,
                        String.valueOf(entry),
                        List.of("MAP<STRING, STRING>"),
                        entry.getClass().toString(),
                        "Must be a map with string keys and string values.");
            }
            validatedMap.put(key, value);
        }
        return validatedMap;
    }
}
