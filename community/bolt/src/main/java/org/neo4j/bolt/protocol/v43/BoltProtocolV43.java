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
package org.neo4j.bolt.protocol.v43;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPIImpl;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v42.BoltProtocolV42;
import org.neo4j.bolt.protocol.v43.fsm.StateMachineV43;
import org.neo4j.bolt.protocol.v43.message.decoder.RouteMessageDecoder;
import org.neo4j.logging.internal.LogService;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.time.SystemNanoClock;

/**
 * Bolt protocol V4.3 It hosts all the components that are specific to BoltV4.3
 */
public class BoltProtocolV43 extends BoltProtocolV42 {
    public static final ProtocolVersion VERSION = new ProtocolVersion(4, 3);

    public BoltProtocolV43(
            LogService logging,
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
            SystemNanoClock clock) {
        super(logging, boltGraphDatabaseManagementServiceSPI, clock);
    }

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }

    @Override
    protected StructRegistry<Connection, RequestMessage> createRequestMessageRegistry() {
        return super.createRequestMessageRegistry()
                .builderOf()
                .register(RouteMessageDecoder.getInstance())
                .build();
    }

    @Override
    public StateMachine createStateMachine(Connection connection) {
        connection.memoryTracker().allocateHeap(StateMachineSPIImpl.SHALLOW_SIZE + StateMachineV43.SHALLOW_SIZE);

        var boltSPI = new StateMachineSPIImpl(logging);
        return new StateMachineV43(boltSPI, connection, clock);
    }
}
