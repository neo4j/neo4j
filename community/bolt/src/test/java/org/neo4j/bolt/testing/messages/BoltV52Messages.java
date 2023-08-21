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
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.message.notifications.DisabledNotificationsConfig;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.authentication.HelloMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.protocol.v52.BoltProtocolV52;

public class BoltV52Messages extends BoltV51Messages {
    private static final String USER_AGENT = "BoltV52Wire/0.0";
    private static final BoltV52Messages INSTANCE = new BoltV52Messages();

    protected BoltV52Messages() {}

    public static BoltMessages getInstance() {
        return INSTANCE;
    }

    @Override
    public ProtocolVersion version() {
        return BoltProtocolV52.VERSION;
    }

    @Override
    public String getUserAgent() {
        return USER_AGENT;
    }

    @Override
    public RequestMessage hello() {
        return new HelloMessage(
                USER_AGENT,
                Collections.emptyList(),
                new RoutingContext(false, Collections.emptyMap()),
                Collections.emptyMap(),
                DisabledNotificationsConfig.getInstance(),
                Collections.emptyMap());
    }
}
