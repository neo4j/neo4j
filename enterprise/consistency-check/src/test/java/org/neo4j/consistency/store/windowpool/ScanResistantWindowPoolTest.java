/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.store.windowpool;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Test;
import org.neo4j.consistency.store.paging.PageReplacementStrategy;
import org.neo4j.consistency.store.paging.StubPageReplacementStrategy;
import org.neo4j.kernel.impl.nioneo.store.OperationType;

public class ScanResistantWindowPoolTest
{
    @Test
    public void shouldMapConsecutiveWindowsWithAppropriateBoundaries() throws Exception
    {
        // given
        int bytesPerRecord = 10;
        int targetBytesPerPage = 1000;
        FileMapper fileMapper = mock( FileMapper.class );
        ScanResistantWindowPool pool = new ScanResistantWindowPool( new File("storeFileName"), bytesPerRecord, targetBytesPerPage,
                fileMapper, new StubPageReplacementStrategy(), 100000, mock( MappingStatisticsListener.class ) );

        // when
        pool.acquire( 0, OperationType.READ );

        // then
        verify( fileMapper ).mapWindow( 0, 100, 10 );

        // when
        pool.acquire( 100, OperationType.READ );

        // then
        verify( fileMapper ).mapWindow( 100, 100, 10 );
    }

    @Test
    public void shouldRejectRecordSizeGreaterThanTargetBytesPerPage() throws Exception
    {
        // given
        int targetBytesPerPage = 4096;
        int bytesPerRecord = targetBytesPerPage + 1;

        // when
        try
        {
            new ScanResistantWindowPool( new File("storeFileName"), bytesPerRecord, targetBytesPerPage,
                    mock( FileMapper.class ), mock( PageReplacementStrategy.class ), 100000, mock( MappingStatisticsListener.class ) );
            fail( "should have thrown exception" );
        }
        // then
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    public void shouldRejectRecordSizeOfZero() throws Exception
    {
        // when
        try
        {
            new ScanResistantWindowPool( new File("storeFileName"), 0, 4096,
                    mock( FileMapper.class ), mock( PageReplacementStrategy.class ), 100000, mock( MappingStatisticsListener.class ) );
            fail( "should have thrown exception" );
        }
        // then
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    public void shouldFailIfFileSizeRequiresMoreThanMaxIntPages() throws Exception
    {
        // given
        int targetBytesPerPage = 1;
        int bytesPerRecord = 1;
        FileMapper fileMapper = mock( FileMapper.class );
        when( fileMapper.fileSizeInBytes() ).thenReturn( Integer.MAX_VALUE * 2L );

        // when
        try
        {
            new ScanResistantWindowPool( new File("storeFileName"), bytesPerRecord, targetBytesPerPage,
                    fileMapper, mock( PageReplacementStrategy.class ), 100000, mock( MappingStatisticsListener.class ) );
            fail( "should have thrown exception" );
        }
        // then
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }
    @Test
    public void shouldRejectWriteOperations() throws Exception
    {
        // given
        int recordSize = 9;
        int targetBytesPerPage = 4096;
        ScanResistantWindowPool pool = new ScanResistantWindowPool( new File("storeFileName"), recordSize, targetBytesPerPage,
                mock( FileMapper.class ), mock( PageReplacementStrategy.class ), 100000, mock( MappingStatisticsListener.class ) );

        // when
        try
        {
            pool.acquire( 0, OperationType.WRITE );
            fail( "should have thrown exception" );
        }
        // then
        catch ( UnsupportedOperationException e )
        {
            // expected
        }
    }

    @Test
    public void closingThePoolShouldCloseAllTheWindows() throws Exception
    {
        // given
        int bytesPerRecord = 10;
        int targetBytesPerPage = 1000;
        FileMapper fileMapper = mock( FileMapper.class );
        MappedWindow window0 = mock( MappedWindow.class );
        when( fileMapper.mapWindow(  0, 100, 10  ) ).thenReturn( window0 );
        MappedWindow window1 = mock( MappedWindow.class );
        when( fileMapper.mapWindow(  100, 100, 10  ) ).thenReturn( window1 );
        ScanResistantWindowPool pool = new ScanResistantWindowPool( new File("storeFileName"), bytesPerRecord, targetBytesPerPage,
                fileMapper, new StubPageReplacementStrategy(), 100000, mock( MappingStatisticsListener.class ) );

        pool.acquire( 0, OperationType.READ );
        pool.acquire( 100, OperationType.READ );

        // when
        pool.close();

        // then
        verify( window0 ).close();
        verify( window1 ).close();
    }

}
