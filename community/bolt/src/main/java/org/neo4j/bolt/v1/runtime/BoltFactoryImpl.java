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
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;
import java.time.Duration;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.udc.UsageData;

public class BoltFactoryImpl extends LifecycleAdapter implements BoltFactory
{
    private final GraphDatabaseAPI gds;
    private final UsageData usageData;
    private final LogService logging;
    private final Authentication authentication;
    private final BoltConnectionTracker connectionTracker;
    private final ThreadToStatementContextBridge txBridge;
    private final Config config;

    private QueryExecutionEngine queryExecutionEngine;
    private GraphDatabaseQueryService queryService;
    private AvailabilityGuard availabilityGuard;

    public BoltFactoryImpl( GraphDatabaseAPI gds, UsageData usageData, LogService logging,
            ThreadToStatementContextBridge txBridge, Authentication authentication,
            BoltConnectionTracker connectionTracker, Config config )
    {
        this.gds = gds;
        this.usageData = usageData;
        this.logging = logging;
        this.txBridge = txBridge;
        this.authentication = authentication;
        this.connectionTracker = connectionTracker;
        this.config = config;
    }

    @Override
    public void start()
    {
        DependencyResolver dependencyResolver = gds.getDependencyResolver();
        queryExecutionEngine = dependencyResolver.resolveDependency( QueryExecutionEngine.class );
        queryService = dependencyResolver.resolveDependency( GraphDatabaseQueryService.class );
        availabilityGuard = dependencyResolver.resolveDependency( AvailabilityGuard.class );
    }

    @Override
    public void stop()
    {
        queryExecutionEngine = null;
        queryService = null;
        availabilityGuard = null;
    }

    @Override
    public BoltStateMachine newMachine( BoltChannel boltChannel, Clock clock )
    {
        TransactionStateMachine.SPI transactionSPI = createTxSpi( clock );
        BoltStateMachine.SPI boltSPI = new BoltStateMachineSPI( boltChannel, usageData,
                logging, authentication, connectionTracker, transactionSPI );
        return new BoltStateMachine( boltSPI, boltChannel, clock, logging );
    }

    private TransactionStateMachine.SPI createTxSpi( Clock clock )
    {
        long bookmarkReadyTimeout = config.get( GraphDatabaseSettings.bookmark_ready_timeout ).toMillis();
        Duration txAwaitDuration = Duration.ofMillis( bookmarkReadyTimeout );

        return new TransactionStateMachineSPI( gds, txBridge, queryExecutionEngine,
                availabilityGuard, queryService, txAwaitDuration, clock );
    }
}
