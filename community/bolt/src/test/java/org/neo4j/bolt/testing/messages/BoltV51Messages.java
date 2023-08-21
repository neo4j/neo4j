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
package org.neo4j.bolt.testing.messages;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.protocol.v51.BoltProtocolV51;
import org.neo4j.bolt.testing.error.UnsupportedProtocolFeatureException;

public class BoltV51Messages extends AbstractBoltMessages {
    private static final String USER_AGENT = "BoltV51Wire/0.0";

    private static final BoltV51Messages INSTANCE = new BoltV51Messages();

    protected BoltV51Messages() {}

    public static BoltMessages getInstance() {
        return INSTANCE;
    }

    @Override
    public ProtocolVersion version() {
        return BoltProtocolV51.VERSION;
    }

    @Override
    public String getUserAgent() {
        return USER_AGENT;
    }

    @Override
    public boolean supportsLogonMessage() {
        return true;
    }

    @Override
    public RequestMessage hello() {
        return this.hello(Collections.emptyList(), new RoutingContext(false, Collections.emptyMap()), null);
    }

    @Override
    public RequestMessage hello(Map<String, Object> authToken) {
        throw new UnsupportedProtocolFeatureException("Authentication via HELLO");
    }

    @Override
    public RequestMessage hello(String principal, String credentials) {
        throw new UnsupportedProtocolFeatureException("Authentication via HELLO");
    }

    @Override
    public RequestMessage hello(List<Feature> features, RoutingContext routingContext, Map<String, Object> authToken) {
        if (authToken != null) {
            throw new UnsupportedProtocolFeatureException("Authentication via HELLO");
        }

        return super.hello(features, routingContext, authToken);
    }
}
