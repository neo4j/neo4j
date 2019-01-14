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
package org.neo4j.consistency.checking.cache;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.consistency.statistics.DefaultCounts;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultClientTest
{

    private static ExecutorService executor;

    @BeforeClass
    public static void setUp()
    {
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterClass
    public static void tearDown()
    {
        executor.shutdown();
    }

    @Test
    public void checkClientsIdBounds() throws ExecutionException, InterruptedException
    {
        int threads = 2;
        DefaultCounts counts = new DefaultCounts( threads );
        DefaultCacheAccess cacheAccess = new DefaultCacheAccess( counts, threads );
        cacheAccess.prepareForProcessingOfSingleStore( 34 );

        CacheAccess.Client client1 = cacheAccess.client();
        assertTrue( client1.withinBounds( 0 ) );
        assertTrue( client1.withinBounds( 10 ) );
        assertTrue( client1.withinBounds( 33 ) );
        assertFalse( client1.withinBounds( 34 ) );

        Future<?> secondClientIdChecks = executor.submit( () ->
        {
            CacheAccess.Client client = cacheAccess.client();
            assertFalse( client.withinBounds( 5 ) );
            assertFalse( client.withinBounds( 33 ) );
            assertTrue( client.withinBounds( 34 ) );
            assertTrue( client.withinBounds( 67 ) );
            assertFalse( client.withinBounds( 68 ) );
        } );
        secondClientIdChecks.get();
    }
}
