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
package org.neo4j.bolt.protocol.v41;

import java.util.function.Predicate;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPIImpl;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v40.BoltProtocolV40;
import org.neo4j.bolt.protocol.v40.transaction.TransactionStateMachineSPIProviderV4;
import org.neo4j.bolt.protocol.v41.fsm.StateMachineV41;
import org.neo4j.bolt.protocol.v41.message.decoder.HelloMessageDecoder;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.internal.LogService;
import org.neo4j.packstream.signal.FrameSignal;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.time.SystemNanoClock;

/**
 * Bolt protocol V4.1 It hosts all the components that are specific to BoltV4.1
 */
public class BoltProtocolV41 extends BoltProtocolV40 {
    public static final ProtocolVersion VERSION = new ProtocolVersion(4, 1);

    public BoltProtocolV41(
            LogService logging,
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
            DefaultDatabaseResolver defaultDatabaseResolver,
            TransactionManager transactionManager,
            SystemNanoClock clock) {
        super(logging, boltGraphDatabaseManagementServiceSPI, defaultDatabaseResolver, transactionManager, clock);
    }

    @Override
    public Predicate<FrameSignal> frameSignalFilter() {
        // all signals are supported
        return signal -> false;
    }

    @Override
    protected StructRegistry<Connection, RequestMessage> createRequestMessageRegistry() {
        return super.createRequestMessageRegistry()
                .builderOf()
                .register(HelloMessageDecoder.getInstance())
                .build();
    }

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }

    @Override
    public StateMachine createStateMachine(Connection connection) {
        connection
                .memoryTracker()
                .allocateHeap(TransactionStateMachineSPIProviderV4.SHALLOW_SIZE
                        + StateMachineSPIImpl.SHALLOW_SIZE
                        + StateMachineV41.SHALLOW_SIZE);

        var transactionSpiProvider =
                new TransactionStateMachineSPIProviderV4(boltGraphDatabaseManagementServiceSPI, connection, clock);
        var boltSPI = new StateMachineSPIImpl(logging, transactionSpiProvider);

        return new StateMachineV41(boltSPI, connection, clock, defaultDatabaseResolver, transactionManager);
    }
}
