/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;
import java.time.Duration;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.TransactionStateMachineSPI;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.udc.UsageData;

public class BoltFactoryImpl implements BoltFactory
{
    private final GraphDatabaseAPI db;
    private final UsageData usageData;
    private final AvailabilityGuard availabilityGuard;
    private final LogService logging;
    private final Authentication authentication;
    private final BoltConnectionTracker connectionTracker;
    private final Config config;

    public BoltFactoryImpl( GraphDatabaseAPI db, UsageData usageData, AvailabilityGuard availabilityGuard,
            Authentication authentication, BoltConnectionTracker connectionTracker, Config config, LogService logging )
    {
        this.db = db;
        this.usageData = usageData;
        this.availabilityGuard = availabilityGuard;
        this.logging = logging;
        this.authentication = authentication;
        this.connectionTracker = connectionTracker;
        this.config = config;
    }

    @Override
    public BoltStateMachine newMachine( BoltChannel boltChannel, Clock clock )
    {
        TransactionStateMachineSPI transactionSPI = createTxSpi( clock );
        BoltStateMachineSPI boltSPI = new BoltStateMachineV1SPI( boltChannel, usageData, logging, authentication, connectionTracker, transactionSPI );
        return new BoltStateMachineV1( boltSPI, boltChannel, clock );
    }

    private TransactionStateMachineSPI createTxSpi( Clock clock )
    {
        long bookmarkReadyTimeout = config.get( GraphDatabaseSettings.bookmark_ready_timeout ).toMillis();
        Duration txAwaitDuration = Duration.ofMillis( bookmarkReadyTimeout );

        return new org.neo4j.bolt.v1.runtime.TransactionStateMachineSPI( db, availabilityGuard, txAwaitDuration, clock );
    }
}
