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

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocolVersion;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.v3.BoltProtocolV3;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.bolt.v3.runtime.TransactionStateMachineSPIProviderV3;
import org.neo4j.bolt.v4.BoltProtocolV4;
import org.neo4j.bolt.v4.BoltStateMachineV4;
import org.neo4j.bolt.v4.runtime.TransactionStateMachineSPIProviderV4;
import org.neo4j.bolt.v41.BoltProtocolV41;
import org.neo4j.bolt.v41.BoltStateMachineV41;
import org.neo4j.bolt.v42.BoltProtocolV42;
import org.neo4j.bolt.v42.BoltStateMachineV42;
import org.neo4j.bolt.v43.BoltProtocolV43;
import org.neo4j.bolt.v43.BoltStateMachineV43;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

public class BoltStateMachineFactoryImpl implements BoltStateMachineFactory
{
    private final BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI;
    private final LogService logging;
    private final Authentication authentication;
    private final SystemNanoClock clock;
    private final DefaultDatabaseResolver defaultDatabaseResolver;

    public BoltStateMachineFactoryImpl( BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI, Authentication authentication,
                                        SystemNanoClock clock, Config config, LogService logging, DefaultDatabaseResolver defaultDatabaseResolver )
    {
        this.boltGraphDatabaseManagementServiceSPI = boltGraphDatabaseManagementServiceSPI;
        this.logging = logging;
        this.authentication = authentication;
        this.clock = clock;
        this.defaultDatabaseResolver = defaultDatabaseResolver;
    }

    @Override
    public BoltStateMachine newStateMachine( BoltProtocolVersion protocolVersion, BoltChannel boltChannel, MapValue connectionHints,
                                             MemoryTracker memoryTracker )
    {
        if ( protocolVersion.equals( BoltProtocolV3.VERSION ) )
        {
            return newStateMachineV3( boltChannel, connectionHints, memoryTracker );
        }
        else if ( protocolVersion.equals( BoltProtocolV4.VERSION ) )
        {
            return newStateMachineV4( boltChannel, connectionHints, memoryTracker );
        }
        else if ( protocolVersion.equals( BoltProtocolV41.VERSION ) )
        {
            return newStateMachineV41( boltChannel, connectionHints, memoryTracker );
        }
        else if ( protocolVersion.equals( BoltProtocolV42.VERSION ) )
        {
            return newStateMachineV42( boltChannel, connectionHints, memoryTracker );
        }
        else if ( protocolVersion.equals( BoltProtocolV43.VERSION ) )
        {
            return newStateMachineV43( boltChannel, connectionHints, memoryTracker );
        }
        else
        {
            throw new IllegalArgumentException( "Failed to create a state machine for protocol version " + protocolVersion );
        }
    }

    private BoltStateMachine newStateMachineV3( BoltChannel boltChannel, MapValue connectionHints, MemoryTracker memoryTracker )
    {
        memoryTracker
                .allocateHeap( TransactionStateMachineSPIProviderV3.SHALLOW_SIZE + BoltStateMachineSPIImpl.SHALLOW_SIZE + BoltStateMachineV3.SHALLOW_SIZE );

        var transactionSpiProvider = new TransactionStateMachineSPIProviderV3( boltGraphDatabaseManagementServiceSPI,
                                                                               boltChannel, clock, memoryTracker );
        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider, boltChannel );
        return new BoltStateMachineV3( boltSPI, boltChannel, clock, defaultDatabaseResolver, connectionHints, memoryTracker );
    }

    private BoltStateMachine newStateMachineV4( BoltChannel boltChannel, MapValue connectionHints, MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap(
                TransactionStateMachineSPIProviderV4.SHALLOW_SIZE + BoltStateMachineSPIImpl.SHALLOW_SIZE + BoltStateMachineV4.SHALLOW_SIZE );

        var transactionSpiProvider = new TransactionStateMachineSPIProviderV4( boltGraphDatabaseManagementServiceSPI,
                                                                               boltChannel, clock, memoryTracker );

        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider, boltChannel );
        return new BoltStateMachineV4( boltSPI, boltChannel, clock, defaultDatabaseResolver, connectionHints, memoryTracker );
    }

    private BoltStateMachine newStateMachineV41( BoltChannel boltChannel, MapValue connectionHints, MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap(
                TransactionStateMachineSPIProviderV4.SHALLOW_SIZE + BoltStateMachineSPIImpl.SHALLOW_SIZE + BoltStateMachineV41.SHALLOW_SIZE );

        var transactionSpiProvider = new TransactionStateMachineSPIProviderV4( boltGraphDatabaseManagementServiceSPI,
                                                                               boltChannel, clock, memoryTracker );
        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider, boltChannel );
        return new BoltStateMachineV41( boltSPI, boltChannel, clock, defaultDatabaseResolver, connectionHints, memoryTracker );
    }

    private BoltStateMachine newStateMachineV42( BoltChannel boltChannel, MapValue connectionHints, MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap(
                TransactionStateMachineSPIProviderV4.SHALLOW_SIZE + BoltStateMachineSPIImpl.SHALLOW_SIZE + BoltStateMachineV42.SHALLOW_SIZE );

        var transactionSpiProvider = new TransactionStateMachineSPIProviderV4( boltGraphDatabaseManagementServiceSPI,
                                                                               boltChannel, clock, memoryTracker );
        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider, boltChannel );
        return new BoltStateMachineV42( boltSPI, boltChannel, clock, defaultDatabaseResolver, connectionHints, memoryTracker );
    }

    private BoltStateMachine newStateMachineV43( BoltChannel boltChannel, MapValue connectionHints, MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap(
                TransactionStateMachineSPIProviderV4.SHALLOW_SIZE + BoltStateMachineSPIImpl.SHALLOW_SIZE + BoltStateMachineV43.SHALLOW_SIZE );

        var transactionSpiProvider = new TransactionStateMachineSPIProviderV4( boltGraphDatabaseManagementServiceSPI,
                                                                               boltChannel, clock, memoryTracker );
        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider, boltChannel );
        return new BoltStateMachineV43( boltSPI, boltChannel, clock, defaultDatabaseResolver, connectionHints, memoryTracker );
    }
}
