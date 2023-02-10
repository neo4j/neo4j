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
package org.neo4j.bolt.testing.messages;

import static java.util.Collections.emptyMap;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

import java.util.Collections;
import java.util.Map;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v41.BoltProtocolV41;
import org.neo4j.bolt.protocol.v41.message.request.HelloMessage;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;

/**
 * Quick access of all Bolt V41 messages
 */
public class BoltV41Messages extends AbstractBoltMessages {
    private static final BoltV41Messages INSTANCE = new BoltV41Messages();

    protected BoltV41Messages() {}

    public static BoltMessages getInstance() {
        return INSTANCE;
    }

    @Override
    public ProtocolVersion version() {
        return BoltProtocolV41.VERSION;
    }

    @Override
    public RequestMessage hello(RoutingContext routingContext) {
        return hello(Collections.emptyMap(), routingContext);
    }

    @Override
    public RequestMessage hello(Map<String, Object> meta) {
        var helloMetaMap = this.getDefaultHelloMetaMap(meta);
        return new HelloMessage(helloMetaMap, new RoutingContext(true, emptyMap()), helloMetaMap);
    }

    @Override
    public RequestMessage hello() {
        return this.hello(new RoutingContext(true, stringMap("policy", "fast", "region", "europe")));
    }

    protected RequestMessage hello(Map<String, Object> meta, RoutingContext routingContext) {
        meta = this.getDefaultHelloMetaMap(meta);
        // TODO: Why
        return new HelloMessage(meta, routingContext, meta);
    }
}
