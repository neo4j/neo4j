/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class ReaderPoolTest
{
    private final File base = new File( "base" );
    private final FileNames fileNames = new FileNames( base );
    private final EphemeralFileSystemAbstraction fsa = spy( new EphemeralFileSystemAbstraction() );
    private final FakeClock clock = Clocks.fakeClock();

    private ReaderPool pool = new ReaderPool( 2, getInstance(), fileNames, fsa, clock );

    @BeforeEach
    public void before()
    {
        fsa.mkdirs( base );
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        fsa.close();
    }

    @Test
    public void shouldReacquireReaderFromPool() throws Exception
    {
        // given
        Reader reader = pool.acquire( 0, 0 );
        pool.release( reader );

        // when
        Reader newReader = pool.acquire( 0, 0 );

        // then
        verify( fsa, times( 1 ) ).open( any(), any() );
        assertThat( reader, is( newReader ) );
    }

    @Test
    public void shouldPruneOldReaders() throws Exception
    {
        // given
        Reader readerA = spy( pool.acquire( 0, 0 ) );
        Reader readerB = spy( pool.acquire( 0, 0 ) );

        pool.release( readerA );

        clock.forward( 2, MINUTES );
        pool.release( readerB );

        // when
        clock.forward( 1, MINUTES );
        pool.prune( 2, MINUTES );

        // then
        verify( readerA ).close();
        verify( readerB, never() ).close();
    }

    @Test
    public void shouldNotReturnPrunedReaders() throws Exception
    {
        Reader readerA = pool.acquire( 0, 0 );
        Reader readerB = pool.acquire( 0, 0 );

        pool.release( readerA );
        pool.release( readerB );

        clock.forward( 2, MINUTES );
        pool.prune( 1, MINUTES );

        // when
        Reader readerC = pool.acquire( 0, 0 );
        Reader readerD = pool.acquire( 0, 0 );

        // then
        assertThat( asSet( readerC, readerD ), not( Matchers.containsInAnyOrder( readerA, readerB ) ) );
    }

    @Test
    public void shouldDisposeSuperfluousReaders() throws Exception
    {
        // given
        Reader readerA = spy( pool.acquire( 0, 0 ) );
        Reader readerB = spy( pool.acquire( 0, 0 ) );
        Reader readerC = spy( pool.acquire( 0, 0 ) );
        Reader readerD = spy( pool.acquire( 0, 0 ) );

        pool.release( readerA );
        pool.release( readerB );

        // when
        pool.release( readerC );
        pool.release( readerD );

        // then
        verify( readerA ).close();
        verify( readerB ).close();
        verify( readerC, never() ).close();
        verify( readerD, never() ).close();
    }

    @Test
    public void shouldDisposeAllReleasedReaders() throws Exception
    {
        // given
        Reader readerA = spy( pool.acquire( 0, 0 ) );
        Reader readerB = spy( pool.acquire( 0, 0 ) );
        Reader readerC = spy( pool.acquire( 0, 0 ) );

        pool.release( readerA );
        pool.release( readerB );
        pool.release( readerC );

        // when
        pool.close();

        // then
        verify( readerA ).close();
        verify( readerB ).close();
        verify( readerC ).close();
    }

    @Test
    public void shouldPruneReadersOfVersion() throws Exception
    {
        // given
        pool = new ReaderPool( 8, getInstance(), fileNames, fsa, clock );

        Reader readerA = spy( pool.acquire( 0, 0 ) );
        Reader readerB = spy( pool.acquire( 1, 0 ) );
        Reader readerC = spy( pool.acquire( 1, 0 ) );
        Reader readerD = spy( pool.acquire( 2, 0 ) );

        pool.release( readerA );
        pool.release( readerB );
        pool.release( readerC );
        pool.release( readerD );

        // when
        pool.prune( 1 );

        // then
        verify( readerA, never() ).close();
        verify( readerB ).close();
        verify( readerC ).close();
        verify( readerD, never() ).close();

        // when
        pool.prune( 0 );
        // then
        verify( readerA ).close();
        verify( readerD, never() ).close();

        // when
        pool.prune( 2 );
        // then
        verify( readerD ).close();
    }
}
