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
package org.neo4j.server.web;

import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.assertEquals;
import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class JettyThreadLimitIT
{
    @Rule
    public SuppressOutput suppressOutput = suppressAll();

    @Test
    public void shouldHaveConfigurableJettyThreadPoolSize() throws Exception
    {
        Jetty9WebServer server = new Jetty9WebServer( NullLogProvider.getInstance(), Config.defaults() );
        int numCores = 1;
        int configuredMaxThreads = 12; // 12 is the new min max Threads value, for one core
        int acceptorThreads = 1; // In this configuration, 1 thread will become an acceptor...
        int selectorThreads = 1; // ... and 1 thread will become a selector...
        int jobThreads = configuredMaxThreads - acceptorThreads - selectorThreads; // ... and the rest are job threads
        server.setMaxThreads( numCores );
        server.setAddress( new ListenSocketAddress( "localhost", PortAuthority.allocatePort() ) );
        try
        {
            server.start();
            QueuedThreadPool threadPool = (QueuedThreadPool) server.getJetty().getThreadPool();
            threadPool.start();
            CountDownLatch startLatch = new CountDownLatch( jobThreads );
            CountDownLatch endLatch = loadThreadPool( threadPool, configuredMaxThreads + 1, startLatch );
            startLatch.await(); // Wait for threadPool to create threads
            int threads = threadPool.getThreads();
            assertEquals( "Wrong number of threads in pool", configuredMaxThreads, threads );
            endLatch.countDown();
        }
        finally
        {
            server.stop();
        }
    }

    private CountDownLatch loadThreadPool( QueuedThreadPool threadPool,
                                           int tasksToSubmit,
                                           final CountDownLatch startLatch )
    {
        CountDownLatch endLatch = new CountDownLatch( 1 );
        for ( int i = 0; i < tasksToSubmit; i++ )
        {
            threadPool.execute( () ->
            {
                startLatch.countDown();
                try
                {
                    endLatch.await();
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
            } );
        }
        return endLatch;
    }
}
