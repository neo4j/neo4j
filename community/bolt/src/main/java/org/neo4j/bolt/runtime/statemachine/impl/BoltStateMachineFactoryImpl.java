/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.v3.runtime.TransactionStateMachineSPIProviderV3;
import org.neo4j.bolt.v4.runtime.TransactionStateMachineSPIProviderV4;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.v3.BoltProtocolV3;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.bolt.v4.BoltProtocolV4;
import org.neo4j.bolt.v4.BoltStateMachineV4;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;

public class BoltStateMachineFactoryImpl implements BoltStateMachineFactory
{
    private final BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI;
    private final LogService logging;
    private final Authentication authentication;
    private final Config config;
    private final SystemNanoClock clock;
    private final String defaultDatabaseName;

    public BoltStateMachineFactoryImpl( BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI, Authentication authentication,
            SystemNanoClock clock, Config config, LogService logging )
    {
        this.boltGraphDatabaseManagementServiceSPI = boltGraphDatabaseManagementServiceSPI;
        this.logging = logging;
        this.authentication = authentication;
        this.config = config;
        this.clock = clock;
        this.defaultDatabaseName = config.get( GraphDatabaseSettings.default_database );
    }

    @Override
    public BoltStateMachine newStateMachine( long protocolVersion, BoltChannel boltChannel )
    {
        if ( protocolVersion == BoltProtocolV3.VERSION )
        {
            return newStateMachineV3( boltChannel );
        }
        else if ( protocolVersion == BoltProtocolV4.VERSION )
        {
            return newStateMachineV4( boltChannel );
        }
        else
        {
            throw new IllegalArgumentException( "Failed to create a state machine for protocol version " + protocolVersion );
        }
    }

    private BoltStateMachine newStateMachineV3( BoltChannel boltChannel )
    {
        var transactionSpiProvider = new TransactionStateMachineSPIProviderV3( boltGraphDatabaseManagementServiceSPI, defaultDatabaseName, boltChannel, clock );
        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider );
        return new BoltStateMachineV3( boltSPI, boltChannel, clock );
    }

    private BoltStateMachine newStateMachineV4( BoltChannel boltChannel )
    {
        var transactionSpiProvider = new TransactionStateMachineSPIProviderV4( boltGraphDatabaseManagementServiceSPI, defaultDatabaseName, boltChannel, clock );
        var boltSPI = new BoltStateMachineSPIImpl( logging, authentication, transactionSpiProvider );
        return new BoltStateMachineV4( boltSPI, boltChannel, clock );
    }
}
