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

import java.util.List;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v43.BoltProtocolV43;
import org.neo4j.bolt.protocol.v43.message.request.RouteMessage;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * Quick access of all Bolt v43 messages
 */
public class BoltV43Messages extends BoltV42Messages {
    private static final BoltV43Messages INSTANCE = new BoltV43Messages();

    protected BoltV43Messages() {}

    public static BoltMessages getInstance() {
        return INSTANCE;
    }

    @Override
    public ProtocolVersion version() {
        return BoltProtocolV43.VERSION;
    }

    @Override
    public RequestMessage route() {
        return new RouteMessage(new MapValueBuilder().build(), List.of(), null);
    }
}
