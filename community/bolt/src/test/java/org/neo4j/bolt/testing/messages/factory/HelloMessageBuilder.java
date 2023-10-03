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
package org.neo4j.bolt.testing.messages.factory;

import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.protocol.v53.BoltProtocolV53;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public final class HelloMessageBuilder
        implements AuthMessageBuilder<HelloMessageBuilder>, NotificationsMessageBuilder<HelloMessageBuilder> {
    private static final short MESSAGE_TAG_HELLO = (short) 0x01;
    private final HashMap<String, Object> meta;
    private final ProtocolVersion version;
    private final String defaultUserAgent;
    private final Set<Feature> defaultFeatures;
    private final Map<String, String> defaultBoltAgent;

    public HelloMessageBuilder(
            ProtocolVersion version,
            String defaultUserAgent,
            Set<Feature> features,
            Map<String, String> defaultBoltAgent) {
        this.meta = new HashMap<>();
        this.version = version;
        this.defaultUserAgent = defaultUserAgent;
        this.defaultBoltAgent = defaultBoltAgent;
        this.defaultFeatures = features;
    }

    public HelloMessageBuilder withUserAgent(Object agent) {
        meta.put("user_agent", agent);
        return this;
    }

    public HelloMessageBuilder withBoltAgent(Map<String, String> agent) {
        meta.put("bolt_agent", agent);
        return this;
    }

    public HelloMessageBuilder withBadBoltAgent(Object agent) {
        meta.put("bolt_agent", agent);
        return this;
    }

    public HelloMessageBuilder withRoutingContext(RoutingContext routingContext) {
        meta.put("routing", routingContext.getParameters());
        return this;
    }

    public HelloMessageBuilder withFeatures(Object features) {
        meta.put("patch_bolt", features);
        return this;
    }

    @Override
    public Map<String, Object> getMeta() {
        return this.meta;
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return this.version;
    }

    @Override
    public ByteBuf build() {
        // From 5.3 bolt_agent is the default & expected value, user agent is explicitly added by the user.
        meta.putIfAbsent("user_agent", defaultUserAgent);
        if (this.version.compareTo(BoltProtocolV53.VERSION) >= 0) {
            meta.putIfAbsent("bolt_agent", defaultBoltAgent);
        }

        if (!this.defaultFeatures.isEmpty()) {
            meta.putIfAbsent(
                    "patch_bolt", defaultFeatures.stream().map(Feature::getId).toList());
        }

        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_HELLO))
                .writeMap(meta)
                .getTarget();
    }
}
