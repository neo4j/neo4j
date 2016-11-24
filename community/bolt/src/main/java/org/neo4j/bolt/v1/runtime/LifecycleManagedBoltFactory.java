/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.udc.UsageData;

public class LifecycleManagedBoltFactory extends LifecycleAdapter implements BoltFactory
{
    private final GraphDatabaseAPI gds;
    private final LifeSupport life = new LifeSupport();
    private final UsageData usageData;
    private final LogService logging;
    private final Authentication authentication;
    private final BoltConnectionTracker connectionTracker;
    private final ThreadToStatementContextBridge txBridge;

    private QueryExecutionEngine queryExecutionEngine;
    private GraphDatabaseQueryService queryService;
    private TransactionIdStore transactionIdStore;
    private AvailabilityGuard availabilityGuard;

    public LifecycleManagedBoltFactory( GraphDatabaseAPI gds, UsageData usageData, LogService logging,
            ThreadToStatementContextBridge txBridge, Authentication authentication,
            BoltConnectionTracker connectionTracker )
    {
        this.gds = gds;
        this.usageData = usageData;
        this.logging = logging;
        this.txBridge = txBridge;
        this.authentication = authentication;
        this.connectionTracker = connectionTracker;
    }

    @Override
    public void init() throws Throwable
    {
        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        DependencyResolver dependencyResolver = gds.getDependencyResolver();
        queryExecutionEngine = dependencyResolver.resolveDependency( QueryExecutionEngine.class );
        queryService = dependencyResolver.resolveDependency( GraphDatabaseQueryService.class );
        transactionIdStore = dependencyResolver.resolveDependency( TransactionIdStore.class );
        availabilityGuard = dependencyResolver.resolveDependency( AvailabilityGuard.class );
        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }

    @Override
    public BoltStateMachine newMachine( String connectionDescriptor, Runnable onClose, Clock clock )
    {
        TransactionStateMachine.SPI transactionSPI =
                new TransactionStateMachineSPI( gds, txBridge, queryExecutionEngine, transactionIdStore,
                        availabilityGuard, queryService, clock );
        BoltStateMachine.SPI boltSPI = new BoltStateMachineSPI( connectionDescriptor, usageData,
                logging, authentication, connectionTracker, transactionSPI );
        return new BoltStateMachine( boltSPI, onClose, Clock.systemUTC() );
    }
}
