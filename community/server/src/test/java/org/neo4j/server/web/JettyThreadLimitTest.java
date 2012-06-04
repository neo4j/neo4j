/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.junit.Test;
import org.mortbay.thread.QueuedThreadPool;

public class JettyThreadLimitTest
{

    @Test
    public void shouldHaveSensibleDefaultJettyThreadPoolSize() throws Exception
    {
    	Jetty6WebServer server = new Jetty6WebServer();
        server.init();
        QueuedThreadPool threadPool = (QueuedThreadPool) server.getJetty()
                .getThreadPool();
        threadPool.start();
        loadThreadPool( threadPool );
        assertEquals( 10 * Runtime.getRuntime()
                .availableProcessors(), threadPool.getThreads() );
        threadPool.stop();
    }

    @Test
    public void shouldHaveConfigurableJettyThreadPoolSize() throws Exception
    {
    	Jetty6WebServer server = new Jetty6WebServer();
        final int maxThreads = 7;
        server.setMaxThreads( maxThreads );
        server.init();
        QueuedThreadPool threadPool = (QueuedThreadPool) server.getJetty()
                .getThreadPool();
        threadPool.start();
        loadThreadPool( threadPool );
        int threads = threadPool.getThreads();
        assertTrue( threads <= maxThreads );
        threadPool.stop();
    }

    private void loadThreadPool( QueuedThreadPool threadPool )
    {
        final CyclicBarrier cb = new CyclicBarrier( 100 );
        for ( int i = 0; i < 100; i++ )
        {
            threadPool.dispatch( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        cb.await();
                    }
                    catch ( InterruptedException e )
                    {
                    }
                    catch ( BrokenBarrierException e )
                    {
                    }
                }
            } );
        }
    }
}
