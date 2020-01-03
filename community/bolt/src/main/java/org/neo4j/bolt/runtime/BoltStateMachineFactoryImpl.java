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
package org.neo4j.bolt.runtime;

import java.time.Clock;
import java.time.Duration;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.v1.BoltProtocolV1;
import org.neo4j.bolt.v1.runtime.BoltStateMachineV1;
import org.neo4j.bolt.v1.runtime.BoltStateMachineV1SPI;
import org.neo4j.bolt.v1.runtime.TransactionStateMachineV1SPI;
import org.neo4j.bolt.v2.BoltProtocolV2;
import org.neo4j.bolt.v3.BoltProtocolV3;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.bolt.v3.runtime.TransactionStateMachineV3SPI;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.internal.LogService;
import org.neo4j.udc.UsageData;

public class BoltStateMachineFactoryImpl implements BoltStateMachineFactory
{
    private final DatabaseManager databaseManager;
    private final UsageData usageData;
    private final LogService logging;
    private final Authentication authentication;
    private final Config config;
    private final Clock clock;
    private final String activeDatabaseName;

    public BoltStateMachineFactoryImpl( DatabaseManager databaseManager, UsageData usageData,
            Authentication authentication, Clock clock, Config config, LogService logging )
    {
        this.databaseManager = databaseManager;
        this.usageData = usageData;
        this.logging = logging;
        this.authentication = authentication;
        this.config = config;
        this.clock = clock;
        this.activeDatabaseName = config.get( GraphDatabaseSettings.active_database );
    }

    @Override
    public BoltStateMachine newStateMachine( long protocolVersion, BoltChannel boltChannel )
    {
        if ( protocolVersion == BoltProtocolV1.VERSION || protocolVersion == BoltProtocolV2.VERSION )
        {
            return newStateMachineV1( boltChannel );
        }
        else if ( protocolVersion == BoltProtocolV3.VERSION )
        {
            return newStateMachineV3( boltChannel );
        }
        else
        {
            throw new IllegalArgumentException( "Failed to create a state machine for protocol version " + protocolVersion );
        }
    }

    private BoltStateMachine newStateMachineV1( BoltChannel boltChannel )
    {
        TransactionStateMachineSPI transactionSPI = new TransactionStateMachineV1SPI( getActiveDatabase(), boltChannel, getAwaitDuration(), clock );
        BoltStateMachineSPI boltSPI = new BoltStateMachineV1SPI( usageData, logging, authentication, transactionSPI );
        return new BoltStateMachineV1( boltSPI, boltChannel, clock );
    }

    private BoltStateMachine newStateMachineV3( BoltChannel boltChannel )
    {
        TransactionStateMachineSPI transactionSPI = new TransactionStateMachineV3SPI( getActiveDatabase(), boltChannel, getAwaitDuration(), clock );
        BoltStateMachineSPI boltSPI = new BoltStateMachineV1SPI( usageData, logging, authentication, transactionSPI );
        return new BoltStateMachineV3( boltSPI, boltChannel, clock );
    }

    private Duration getAwaitDuration()
    {
        long bookmarkReadyTimeout = config.get( GraphDatabaseSettings.bookmark_ready_timeout ).toMillis();

        return Duration.ofMillis( bookmarkReadyTimeout );
    }

    private GraphDatabaseFacade getActiveDatabase()
    {
        return databaseManager.getDatabaseFacade( activeDatabaseName ).get();
    }
}
