/**
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
package org.neo4j.server.web;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.test.Mute;

import static org.junit.Assert.assertTrue;

import static org.neo4j.test.Mute.muteAll;

public class JettyThreadLimitTest
{
    @Rule
    public Mute mute = muteAll();

    @Test
    public void shouldHaveConfigurableJettyThreadPoolSize() throws Exception
    {
    	  Jetty9WebServer server = new Jetty9WebServer( DevNullLoggingService.DEV_NULL );
        final int maxThreads = 7;
        server.setMaxThreads( maxThreads );
        server.setPort( 7480 );
        try {
	        server.start();
	        QueuedThreadPool threadPool = (QueuedThreadPool) server.getJetty()
	                .getThreadPool();
	        threadPool.start();
            int configuredMaxThreads = maxThreads * Runtime.getRuntime().availableProcessors();
            loadThreadPool( threadPool, configuredMaxThreads + 1);
	        int threads = threadPool.getThreads();
	        assertTrue( threads <= maxThreads );
        }
        finally
        {
        	server.stop();
        }
    }

    private void loadThreadPool(QueuedThreadPool threadPool, int tasksToSubmit)
    {
        final CyclicBarrier cb = new CyclicBarrier(tasksToSubmit);
        for ( int i = 0; i < tasksToSubmit; i++ )
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
                        e.printStackTrace();
                    }
                    catch ( BrokenBarrierException e )
                    {
                        e.printStackTrace();
                    }
                }
            } );
        }
    }
}
