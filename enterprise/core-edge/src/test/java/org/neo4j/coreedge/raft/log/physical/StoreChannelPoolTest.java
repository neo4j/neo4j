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
package org.neo4j.coreedge.raft.log.physical;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.logging.NullLogProvider;

public class StoreChannelPoolTest
{
    @Test
    public void shouldSignalWhenPoolDepleted() throws Exception
    {
        // given
        FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        StoreChannelPool pool = new StoreChannelPool( fsa, new File( "raft-log" ), "rw", NullLogProvider.getInstance() );
        StoreChannel channel = pool.acquire( 0 );

        CountDownLatch latch = new CountDownLatch( 1 );
        Runnable onDisposal = latch::countDown;

        pool.markForDisposal( onDisposal );

        // when
        pool.release( channel );

        // then
        assertTrue( latch.await( 10, SECONDS ) );
    }

    @Test
    public void shouldNotBeAbleToAcquireFromPoolMarkedForDisposal() throws Exception
    {
        // given
        FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        StoreChannelPool pool = new StoreChannelPool( fsa, new File( "raft-log" ), "rw", NullLogProvider.getInstance() );
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
