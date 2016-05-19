/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.log.segmented;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.coreedge.raft.log.segmented.SegmentedRaftLog.SEGMENTED_LOG_DIRECTORY_NAME;
import static org.neo4j.coreedge.raft.log.segmented.StoreChannelPool.CLOSED_ERROR_MESSAGE;

public class StoreChannelPoolTest
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final File file = new File( SEGMENTED_LOG_DIRECTORY_NAME );
    private final NullLogProvider logProvider = NullLogProvider.getInstance();

    @Test
    public void shouldSignalWhenPoolDeleted() throws Exception
    {
        // given
        try ( StoreChannelPool pool = new StoreChannelPool( fsRule.get(), file, "rw", logProvider ) )
        {
            StoreChannel channel = pool.acquire( 0 );

            CountDownLatch latch = new CountDownLatch( 1 );
            Runnable onDisposal = latch::countDown;

            pool.markForDisposal( onDisposal );

            // when
            pool.release( channel );

            // then
            assertTrue( latch.await( 10, SECONDS ) );
        }
    }

    @Test
    public void shouldNotBeAbleToAcquireFromPoolMarkedForDisposal() throws Exception
    {
        // given
        try ( StoreChannelPool pool = new StoreChannelPool( fsRule.get(), file, "rw", logProvider ) )
        {
            pool.markForDisposal( () -> {} );

            // when
            try
            {
                pool.acquire( 0 );
                fail();
            }
            catch ( DisposedException pde )
            {
                // then
            }
        }
    }

    @Test
    public void shouldBeAbleToCloseThePool() throws Exception
    {
        // given
        StoreChannelPool pool = new StoreChannelPool( fsRule.get(), file, "rw", logProvider );
        pool.close();

        // when
        try
        {
            pool.acquire( 0 );
            fail();
        }
        catch ( RuntimeException ex )
        {
            // then
            assertEquals( CLOSED_ERROR_MESSAGE, ex.getMessage() );
        }
    }
}
