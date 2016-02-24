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
package org.neo4j.bolt.v1.transport.integration;

import org.junit.After;
import org.junit.Test;

import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.auth.AuthManager;
import org.neo4j.udc.UsageData;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.BoltKernelExtension.Settings.connector;
import static org.neo4j.bolt.BoltKernelExtension.Settings.enabled;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class NettyLoggingIT
{
    private LifeSupport life;

    @Test
    public void shouldEnableNettyDebugLoggingIfAsked() throws Throwable
    {
        // Given
        AssertableLogProvider log = new AssertableLogProvider( true );

        Config config = new Config(stringMap(
            connector( 0, enabled ).name(), "true"
        ));

        life = new LifeSupport();
        life.add( new BoltKernelExtension().newInstance(
                    mock( KernelContext.class ),
                    stubDependencies( config, log ) ) );

        // When
        life.start();

        // Then
        log.assertAtLeastOnce(
            // Depending on netty internals here - point is to assert on something
            // that shows we're getting netty debug logging coming through. If
            // we upgrade netty and this changes, just assert some other debug statement
            // from netty shows up.
            inLog("io.netty.buffer.PooledByteBufAllocator")
                .debug(
                    equalTo( "-Dio.netty.allocator.cacheTrimInterval: %s"),
                    greaterThan( 0 ) ) );
    }

    @After
    public void cleanup()
    {
        life.shutdown();
    }

    private BoltKernelExtension.Dependencies stubDependencies( final Config config, final AssertableLogProvider log )
    {
        return new BoltKernelExtension.Dependencies()
        {
            @Override
            public LogService logService()
            {
                return new SimpleLogService( log, log );
            }

            @Override
            public Config config()
            {
                return config;
            }

            @Override
            public GraphDatabaseService db()
            {
                return mock( GraphDatabaseAPI.class );
            }

            @Override
            public JobScheduler scheduler()
            {
                return mock( JobScheduler.class );
            }

            @Override
            public UsageData usageData()
            {
                return new UsageData();
            }

            @Override
            public Monitors monitors()
            {
                return new Monitors();
            }

            @Override
            public AuthManager authManager()
            {
                return mock(AuthManager.class);
            }

            @Override
            public ThreadToStatementContextBridge txBridge()
            {
                return mock(ThreadToStatementContextBridge.class);
            }

            @Override
            public QueryExecutionEngine queryEngine()
            {
                return mock( QueryExecutionEngine.class );
            }
        };
    }
}
