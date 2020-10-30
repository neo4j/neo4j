/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.time.SystemNanoClock;

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
    public BoltStateMachine newStateMachine( BoltProtocolVersion protocolVersion, BoltChannel boltChannel )
    {
        if ( protocolVersion.equals( BoltProtocolV3.VERSION ) )
        {
            return newStateMachineV3( boltChannel );
        }
        else if ( protocolVersion.equals( BoltProtocolV4.VERSION ) )
        {
            return newStateMachineV4( boltChannel );
        }
        else if ( protocolVersion.equals( BoltProtocolV41.VERSION ) )
        {
            return newStateMachineV41( boltChannel );
        }
        else if ( protocolVersion.equals( BoltProtocolV42.VERSION ) )
        {
            return newStateMachineV42( boltChannel );
        }
        else if ( protocolVersion.equals( BoltProtocolV43.VERSION ) )
        {
            return newStateMachineV43( boltChannel );
        }
        else
        {
            throw new IllegalArgumentException( "Failed to create a state machine for protocol version " + protocolVersion );
        }
    }

    private BoltStateMachine newStateMachineV3( BoltChannel boltChannel )
    {
        var transactionSpiProvider = new TransactionStateMachineSPIProviderV3( boltGraphDatabaseManagementServiceSPI,
                                                                               boltChannel, clock );
        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider );
        return new BoltStateMachineV3( boltSPI, boltChannel, clock, defaultDatabaseResolver );
    }

    private BoltStateMachine newStateMachineV4( BoltChannel boltChannel )
    {
        var transactionSpiProvider = new TransactionStateMachineSPIProviderV4( boltGraphDatabaseManagementServiceSPI,
                                                                               boltChannel, clock );
        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider );
        return new BoltStateMachineV4( boltSPI, boltChannel, clock, defaultDatabaseResolver );
    }

    private BoltStateMachine newStateMachineV41( BoltChannel boltChannel )
    {
        var transactionSpiProvider = new TransactionStateMachineSPIProviderV4( boltGraphDatabaseManagementServiceSPI,
                                                                               boltChannel, clock );
        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider );
        return new BoltStateMachineV41( boltSPI, boltChannel, clock, defaultDatabaseResolver );
    }

    private BoltStateMachine newStateMachineV42( BoltChannel boltChannel )
    {
        var transactionSpiProvider = new TransactionStateMachineSPIProviderV4( boltGraphDatabaseManagementServiceSPI,
                                                                               boltChannel, clock );
        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider );
        return new BoltStateMachineV42( boltSPI, boltChannel, clock, defaultDatabaseResolver );
    }

    private BoltStateMachine newStateMachineV43( BoltChannel boltChannel )
    {
        var transactionSpiProvider = new TransactionStateMachineSPIProviderV4( boltGraphDatabaseManagementServiceSPI,
                                                                               boltChannel, clock );
        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider );
        return new BoltStateMachineV43( boltSPI, boltChannel, clock, defaultDatabaseResolver );
    }
}
