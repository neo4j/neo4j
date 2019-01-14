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
package org.neo4j.server;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.test.ThreadTestUtils;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.junit.Assert.fail;

public abstract class NeoServerRestartTestIT extends ExclusiveServerTestBase
{
    public static final String CUSTOM_SWAPPER = "CustomSwapper";
    private static Semaphore semaphore;

    static
    {
        semaphore = new Semaphore( 0 );
    }

    /**
     * This test makes sure that the database is able to start after having been stopped during initialization.
     *
     * In order to make sure that the server is stopped during startup we create a separate thread that calls stop.
     * In order to make sure that this thread does not call stop before the startup procedure has started we use a
     * custom implementation of a PageSwapperFactory, which communicates with the thread that calls stop. We do this
     * via a static semaphore.
     * @throws IOException
     * @throws InterruptedException
     */

    @Test
    public void shouldBeAbleToRestartWhenStoppedDuringStartup() throws IOException, InterruptedException
    {
        // Make sure that the semaphore is in a clean state.
        semaphore.drainPermits();
        // Get a server that uses our custom swapper.
        NeoServer server = getNeoServer( CUSTOM_SWAPPER );

        try
        {
            AtomicBoolean failure = new AtomicBoolean();
            Thread serverStoppingThread = ThreadTestUtils.fork( stopServerAfterStartingHasStarted( server, failure ) );
            server.start();
            // Wait for the server to stop.
            serverStoppingThread.join();
            // Check if the server stopped successfully.
            if ( failure.get() )
            {
                fail( "Server failed to stop." );
            }
            // Verify that we can start the server again.
            server = getNeoServer( CUSTOM_SWAPPER );
            server.start();
        }
        finally
        {
            server.stop();
        }
    }

    protected abstract NeoServer getNeoServer( String customPageSwapperName ) throws IOException;

    private Runnable stopServerAfterStartingHasStarted( NeoServer server, AtomicBoolean failure )
    {
        return () ->
        {
            try
            {
                // Make sure that we have started the startup procedure before calling stop.
                semaphore.acquire();
                server.stop();
            }
            catch ( Exception e )
            {
                failure.set( true );
            }
        };
    }

    // This class is used to notify the test that the server has started its startup procedure.
    public static class CustomSwapper extends SingleFilePageSwapperFactory
    {
        @Override
        public String implementationName()
        {
            return CUSTOM_SWAPPER;
        }

        @Override
        public PageSwapper createPageSwapper( File file, int filePageSize, PageEvictionCallback onEviction,
                boolean createIfNotExist ) throws IOException
        {
            // This will be called early in the startup sequence. Notifies that we can call stop on the server.
            semaphore.release();
            return super.createPageSwapper( file, filePageSize, onEviction, createIfNotExist );
        }
    }
}
