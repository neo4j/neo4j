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
package org.neo4j.server.queryapi.driver.boltmessage.pipeline;

import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.ACCESS_MODE_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.AUTH_KEYS;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.BEARER_VALUE;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.BOLT_AGENT_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.BOOKMARKS_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.CREDENTIALS_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.DATABASE_NAME_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.EXPLICIT_TX_VALUE;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.IMPERSONATED_USER_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.IMPLICIT_TX_VALUE;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.NUMBER_OF_RECORDS_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.PATCH_BOLT_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.PRINCIPAL_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.QUERY_ID_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.READ_ACCESS_MODE_VALUE;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.ROUTING_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.SCHEME_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.TX_METADATA_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.TX_TIMEOUT_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.TX_TYPE_KEY;
import static org.neo4j.server.queryapi.driver.boltmessage.pipeline.BoltFields.USER_AGENT_KEY;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.connection.netty.impl.messaging.request.CommitMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.GoodbyeMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.LogoffMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.LogonMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.RequestMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.ResetMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.RollbackMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.RunWithMetadataMessage;
import org.neo4j.bolt.connection.values.Type;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.boltmessages.AccessMode;
import org.neo4j.boltmessages.TransactionType;
import org.neo4j.boltmessages.notifications.DefaultNotificationsConfig;
import org.neo4j.boltmessages.request.authentication.HelloMessage;
import org.neo4j.boltmessages.request.connection.RoutingContext;
import org.neo4j.boltmessages.request.streaming.DiscardMessage;
import org.neo4j.boltmessages.request.streaming.PullMessage;
import org.neo4j.boltmessages.request.transaction.BeginMessage;
import org.neo4j.boltmessages.request.transaction.RunMessage;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.server.queryapi.driver.boltmessage.codec.BoltMessageValueEncoder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

public class OutboundMessageEncoder extends MessageToMessageEncoder<RequestMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RequestMessage msg, List<Object> out) throws Exception {
        out.add(encode(msg));
    }

    public static org.neo4j.boltmessages.request.RequestMessage encode(
            org.neo4j.bolt.connection.netty.impl.messaging.request.RequestMessage message) {
        return switch (message) {
            case org.neo4j.bolt.connection.netty.impl.messaging.request.HelloMessage helloMessage -> {
                var metadata = helloMessage.metadata();
                yield new HelloMessage(
                        stringValue(metadata, USER_AGENT_KEY),
                        stringList(metadata, PATCH_BOLT_KEY),
                        new RoutingContext(true, stringMap(metadata, ROUTING_KEY)),
                        authToken(metadata),
                        DefaultNotificationsConfig.getInstance(),
                        stringMap(metadata, BOLT_AGENT_KEY));
            }
            case LogonMessage logonMessage ->
                new org.neo4j.boltmessages.request.authentication.LogonMessage(
                        convertAuthToken(logonMessage.metadata()));
            case LogoffMessage logoffMessage ->
                org.neo4j.boltmessages.request.authentication.LogoffMessage.getInstance();
            case ResetMessage resetMessage -> org.neo4j.boltmessages.request.connection.ResetMessage.getInstance();
            case GoodbyeMessage goodbyeMessage ->
                org.neo4j.boltmessages.request.connection.GoodbyeMessage.getInstance();
            case RunWithMetadataMessage runMessage ->
                new RunMessage(
                        runMessage.query(),
                        toMapValue(runMessage.parameters()),
                        stringList(runMessage.metadata(), BOOKMARKS_KEY),
                        txTimeout(runMessage.metadata()),
                        accessMode(runMessage.metadata()),
                        txMetadata(runMessage.metadata()),
                        databaseName(runMessage.metadata()),
                        impersonatedUser(runMessage.metadata()),
                        null);
            case org.neo4j.bolt.connection.netty.impl.messaging.request.BeginMessage beginMessage ->
                new BeginMessage(
                        stringList(beginMessage.metadata(), BOOKMARKS_KEY),
                        txTimeout(beginMessage.metadata()),
                        accessMode(beginMessage.metadata()),
                        txMetadata(beginMessage.metadata()),
                        databaseName(beginMessage.metadata()),
                        impersonatedUser(beginMessage.metadata()),
                        beginMessage
                                        .metadata()
                                        .getOrDefault(TX_TYPE_KEY, new StringValue(EXPLICIT_TX_VALUE))
                                        .asString()
                                        .equals(IMPLICIT_TX_VALUE)
                                ? TransactionType.IMPLICIT
                                : TransactionType.EXPLICIT,
                        null);
            case CommitMessage commitMessage -> org.neo4j.boltmessages.request.transaction.CommitMessage.getInstance();
            case RollbackMessage rollbackMessage ->
                org.neo4j.boltmessages.request.transaction.RollbackMessage.getInstance();
            case org.neo4j.bolt.connection.netty.impl.messaging.request.PullMessage pullMessage ->
                new PullMessage(
                        pullMessage.metadata().get(NUMBER_OF_RECORDS_KEY).asLong(),
                        pullMessage
                                .metadata()
                                .getOrDefault(QUERY_ID_KEY, new IntegerValue(-1))
                                .asLong());
            case org.neo4j.bolt.connection.netty.impl.messaging.request.DiscardMessage discardMessage ->
                new DiscardMessage(
                        discardMessage.metadata().get(NUMBER_OF_RECORDS_KEY).asLong(),
                        discardMessage.metadata().get(QUERY_ID_KEY).asLong());
            default -> throw new IllegalStateException("Unexpected driver message: " + message);
        };
    }

    private static MapValue toMapValue(Map<String, Value> map) {
        var convertedMap = new MapValueBuilder();
        map.forEach((k, v) -> convertedMap.add(k, BoltMessageValueEncoder.encode(v)));
        return convertedMap.build();
    }

    private static Map<String, Object> convertAuthToken(Map<String, ?> metadata) {
        var convertedMap = new HashMap<String, Object>();
        convertedMap.put(SCHEME_KEY, ((StringValue) metadata.get(SCHEME_KEY)).asString());
        var scheme = (String) convertedMap.get(SCHEME_KEY);

        if (scheme.equals(BEARER_VALUE)) {
            convertedMap.put(
                    CREDENTIALS_KEY,
                    ((StringValue) metadata.get(CREDENTIALS_KEY)).asString().getBytes(StandardCharsets.UTF_8));
        } else if (scheme.equals(BoltFields.BASIC_VALUE)) {
            convertedMap.put(PRINCIPAL_KEY, ((StringValue) metadata.get(PRINCIPAL_KEY)).asString());
            convertedMap.put(
                    CREDENTIALS_KEY,
                    ((StringValue) metadata.get(CREDENTIALS_KEY)).asString().getBytes(StandardCharsets.UTF_8));
        }
        return convertedMap;
    }

    private static Object toJavaObject(Object value) {
        // Note, we only need a subset of types here for auth token
        if (value instanceof Value driverValue) {
            switch (driverValue.boltValueType()) {
                case Type.STRING -> {
                    return driverValue.asString();
                }
                case Type.MAP -> {
                    var objMap = new HashMap<>();
                    driverValue.asBoltMap().forEach((k, v) -> objMap.put(k, toJavaObject(v)));
                    return objMap;
                }
                default -> throw new UnsupportedOperationException("Unexpected driver value type");
            }
        }
        throw new IllegalStateException("Unexpected driver value type");
    }

    private static String stringValue(Map<String, ?> metadata, String key) {
        var value = metadata.get(key);
        return value instanceof Value driverValue ? driverValue.asString() : (String) value;
    }

    @SuppressWarnings("unchecked")
    private static List<String> stringList(Map<String, ?> metadata, String key) {
        var value = metadata.get(key);
        if (value == null) {
            return List.of();
        }
        if (value instanceof Value driverValue) {
            var patchBoltList = new ArrayList<String>();
            driverValue.boltValues().forEach(v -> patchBoltList.add(v.asString()));
            return patchBoltList;
        }
        return List.of();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> stringMap(Map<String, ?> metadata, String key) {
        var value = metadata.get(key);
        if (value == null) {
            return Map.of();
        }
        if (value instanceof Value driverValue) {
            var stringMap = new HashMap<String, String>();
            driverValue.asBoltMap().forEach((k, v) -> stringMap.put(k, v.asString()));
            return stringMap;
        }
        return Map.of();
    }

    private static Map<String, Object> authToken(Map<String, ?> metadata) {
        var authToken = new HashMap<String, Object>();
        metadata.forEach((key, value) -> {
            if (AUTH_KEYS.contains(key)) {
                authToken.put(key, toJavaObject(value));
            }
        });
        return authToken;
    }

    private static Duration txTimeout(Map<String, ?> metadata) {
        var value = metadata.get(TX_TIMEOUT_KEY);
        if (value == null) {
            return null;
        }
        long millis = value instanceof Value driverValue ? driverValue.asLong() : ((Number) value).longValue();
        return Duration.ofMillis(millis);
    }

    private static AccessMode accessMode(Map<String, ?> metadata) {
        var value = metadata.get(ACCESS_MODE_KEY);
        if (value == null) {
            return AccessMode.WRITE;
        }
        var mode = value instanceof Value driverValue ? driverValue.asString() : value.toString();
        return READ_ACCESS_MODE_VALUE.equals(mode) ? AccessMode.READ : AccessMode.WRITE;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> txMetadata(Map<String, ?> metadata) {
        var value = metadata.get(TX_METADATA_KEY);
        if (value == null) {
            return Map.of();
        }
        if (value instanceof Value driverValue) {
            var metadataMap = new HashMap<String, Object>();
            driverValue.asBoltMap().forEach((k, v) -> metadataMap.put(k, BoltMessageValueEncoder.encode(v)));
            return metadataMap;
        }
        return Map.of();
    }

    private static String databaseName(Map<String, ?> metadata) {
        return optionalString(metadata, DATABASE_NAME_KEY);
    }

    private static String impersonatedUser(Map<String, ?> metadata) {
        return optionalString(metadata, IMPERSONATED_USER_KEY);
    }

    private static String optionalString(Map<String, ?> metadata, String key) {
        var value = metadata.get(key);
        if (value == null) {
            return null;
        }
        return value instanceof Value driverValue ? driverValue.asString() : value.toString();
    }
}
