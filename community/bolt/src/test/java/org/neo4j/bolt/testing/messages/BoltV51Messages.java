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
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.bolt.protocol.v44.message.request.RouteMessage;
import org.neo4j.bolt.protocol.v51.message.request.HelloMessage;
import org.neo4j.bolt.protocol.v51.message.request.LogoffMessage;
import org.neo4j.bolt.protocol.v51.message.request.LogonMessage;
import org.neo4j.values.virtual.MapValueBuilder;

public class BoltV51Messages extends BoltV50Messages {
    private static final String USER_AGENT = "BoltV51Wire/0.0";

    private static final BoltV51Messages INSTANCE = new BoltV51Messages();

    protected BoltV51Messages() {}

    public static BoltMessages getInstance() {
        return INSTANCE;
    }

    private static final RequestMessage HELLO = new HelloMessage(
            map("user_agent", USER_AGENT), new RoutingContext(true, stringMap("policy", "fast", "region", "europe")));

    public static RouteMessage route(String impersonatedUser) {
        return new RouteMessage(new MapValueBuilder().build(), List.of(), null, impersonatedUser);
    }

    @Override
    public RequestMessage logon() {
        return new LogonMessage(new HashMap<>(0));
    }

    @Override
    public RequestMessage logoff() {
        return LogoffMessage.INSTANCE;
    }

    @Override
    public RequestMessage hello(Map<String, Object> meta) {
        return new HelloMessage(this.getDefaultHelloMetaMap(meta), new RoutingContext(true, emptyMap()));
    }

    @Override
    public RequestMessage hello() {
        return new HelloMessage(this.getDefaultHelloMetaMap(emptyMap()), new RoutingContext(true, emptyMap()));
    }
}
