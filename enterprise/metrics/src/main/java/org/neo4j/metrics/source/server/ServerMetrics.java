/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.metrics.source.server;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.DependencySatisfier;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Server Metrics" )
public class ServerMetrics extends LifecycleAdapter
{
    private static final String NAME_PREFIX = "neo4j.server";

    @Documented( "The total number of idle threads in the jetty pool" )
    public static final String THREAD_JETTY_IDLE = name( NAME_PREFIX, "threads.jetty.idle" );
    @Documented( "The total number of threads (both idle and busy) in the jetty pool" )
    public static final String THREAD_JETTY_ALL = name( NAME_PREFIX, "threads.jetty.all" );

    private final MetricRegistry registry;
    private volatile ServerThreadView serverThreadView;

    public ServerMetrics( MetricRegistry registry, LogService logService, DependencySatisfier satisfier )
    {
        Log userLog = logService.getUserLog( getClass() );
        this.registry = registry;
        this.serverThreadView = new ServerThreadView()
        {
            @Override
            public int idleThreads()
            {
                userLog.warn( "Server thread metrics not available (missing " + THREAD_JETTY_IDLE + ")" );
                return -1;
            }

            @Override
            public int allThreads()
            {
                userLog.warn( "Server thread metrics not available (missing " + THREAD_JETTY_ALL + ")" );
                return -1;
            }
        };
        satisfier.satisfyDependency( (ServerThreadViewSetter) serverThreadView ->
        {
            assert ServerMetrics.this.serverThreadView != null;
            ServerMetrics.this.serverThreadView = serverThreadView;
            userLog.info( "Server thread metrics have been registered successfully" );
        } );
    }

    @Override
    public void start()
    {
        registry.register( THREAD_JETTY_IDLE, (Gauge<Integer>) () -> serverThreadView.idleThreads() );
        registry.register( THREAD_JETTY_ALL, (Gauge<Integer>) () -> serverThreadView.allThreads() );
    }

    @Override
    public void stop()
    {
        registry.remove( THREAD_JETTY_IDLE );
        registry.remove( THREAD_JETTY_ALL );
    }
}
