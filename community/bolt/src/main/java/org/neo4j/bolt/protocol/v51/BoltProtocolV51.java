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
package org.neo4j.bolt.protocol.v51;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPIImpl;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v50.BoltProtocolV50;
import org.neo4j.bolt.protocol.v51.fsm.StateMachineV51;
import org.neo4j.bolt.protocol.v51.message.decoder.HelloMessageDecoder;
import org.neo4j.bolt.protocol.v51.message.decoder.LogoffMessageDecoder;
import org.neo4j.bolt.protocol.v51.message.decoder.LogonMessageDecoder;
import org.neo4j.logging.internal.LogService;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.time.SystemNanoClock;

public class BoltProtocolV51 extends BoltProtocolV50 {
    public static final ProtocolVersion VERSION = new ProtocolVersion(5, 1);

    public BoltProtocolV51(
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
                .register(LogoffMessageDecoder.getInstance())
                .register(LogonMessageDecoder.getInstance())
                .register(HelloMessageDecoder.getInstance())
                .build();
    }

    @Override
    public StateMachine createStateMachine(Connection connection) {
        connection.memoryTracker().allocateHeap(StateMachineSPIImpl.SHALLOW_SIZE + StateMachineV51.SHALLOW_SIZE);

        var boltSPI = new StateMachineSPIImpl(logging);

        return new StateMachineV51(boltSPI, connection, clock);
    }
}
