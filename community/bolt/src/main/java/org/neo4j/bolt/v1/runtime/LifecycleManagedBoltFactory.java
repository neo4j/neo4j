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

import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.v1.runtime.cypher.CypherStatementRunner;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
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
    private final NeoStoreDataSource neoStoreDataSource;
    private final ThreadToStatementContextBridge txBridge;

    private QueryExecutionEngine queryExecutionEngine;

    public LifecycleManagedBoltFactory( GraphDatabaseAPI gds, UsageData usageData, LogService logging,
                                        ThreadToStatementContextBridge txBridge, Authentication authentication,
                                        NeoStoreDataSource neoStoreDataSource, BoltConnectionTracker connectionTracker)
    {
        this.gds = gds;
        this.usageData = usageData;
        this.logging = logging;
        this.txBridge = txBridge;
        this.authentication = authentication;
        this.connectionTracker = connectionTracker;
        this.neoStoreDataSource = neoStoreDataSource;
    }

    @Override
    public void init() throws Throwable
    {
        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        this.queryExecutionEngine = gds.getDependencyResolver().resolveDependency( QueryExecutionEngine.class );
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
    public BoltStateMachine newMachine( String connectionDescriptor, Runnable onClose )
    {
        final CypherStatementRunner statementRunner = new CypherStatementRunner( queryExecutionEngine, txBridge );
        BoltStateMachine.SPI spi = new BoltStateMachineSPI( connectionDescriptor, usageData, gds,
                queryExecutionEngine, logging, authentication, txBridge, statementRunner, connectionTracker );
        return new BoltStateMachine( spi, onClose );
    }
}
