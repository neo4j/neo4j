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
package org.neo4j.bolt.runtime.statemachine.impl;

import java.time.Clock;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPIProvider;
import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.memory.MemoryTracker;

public class StatementProcessorProvider
{
    private final Clock clock;
    private final TransactionStateMachineSPIProvider spiProvider;
    private final StatementProcessorReleaseManager resourceReleaseManager;
    private final RoutingContext routingContext;
    private final MemoryTracker memoryTracker;

    public StatementProcessorProvider( TransactionStateMachineSPIProvider transactionSpiProvider, Clock clock,
                                       StatementProcessorReleaseManager releaseManager, RoutingContext routingContext, MemoryTracker memoryTracker )
    {
        this.spiProvider = transactionSpiProvider;
        this.clock = clock;
        this.resourceReleaseManager = releaseManager;
        this.routingContext = routingContext;
        this.memoryTracker = memoryTracker;
    }

    public StatementProcessor getStatementProcessor( LoginContext loginContext, String databaseName, String txId )
            throws BoltProtocolBreachFatality, BoltIOException
    {
        memoryTracker.allocateHeap( TransactionStateMachine.SHALLOW_SIZE );

        TransactionStateMachineSPI transactionSPI = spiProvider.getTransactionStateMachineSPI( databaseName, resourceReleaseManager, txId );
        return new TransactionStateMachine( databaseName, transactionSPI, loginContext, clock, routingContext, txId );
    }

    public void releaseStatementProcessor()
    {
        memoryTracker.releaseHeap( TransactionStateMachine.SHALLOW_SIZE );

        spiProvider.releaseTransactionStateMachineSPI();
    }
}
