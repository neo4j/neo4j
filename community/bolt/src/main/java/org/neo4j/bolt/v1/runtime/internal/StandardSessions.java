/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.internal;

import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.Sessions;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.udc.UsageData;

/**
 * The runtime environment in which user statements are executed. This is not a thread safe class, it is expected
 * that a single worker thread will use this environment.
 */
public class StandardSessions extends LifecycleAdapter implements Sessions
{
    private final GraphDatabaseAPI gds;
    private final LifeSupport life = new LifeSupport();
    private final UsageData usageData;
    private final LogService logging;

    private CypherStatementRunner statementRunner;
    private ThreadToStatementContextBridge txBridge;

    public StandardSessions( GraphDatabaseAPI gds, UsageData usageData, LogService logging )
    {
        this.gds = gds;
        this.usageData = usageData;
        this.logging = logging;
        // TODO: Introduce a clean SPI rather than use GDS
        this.txBridge = gds.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }

    @Override
    public void init() throws Throwable
    {
        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        QueryExecutionEngine queryExecutionEngine = gds.getDependencyResolver().resolveDependency( QueryExecutionEngine.class );
        statementRunner = new CypherStatementRunner( gds, queryExecutionEngine );
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
    public Session newSession( boolean isEncrypted )
    {
        return new SessionStateMachine( usageData, gds, txBridge, statementRunner, logging );
    }
}
