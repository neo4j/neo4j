/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.standard;

import java.io.File;

import org.junit.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PagedFile;

import static org.mockito.Mockito.*;

public class StandardPageCacheTest
{
    @Test
    public void shouldCloseAllFiles() throws Exception
    {
        // Given
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        StandardPageCache cache = new StandardPageCache( fs, 16, 512 );
        File file1Name = new File( "file1" );
        File file2Name = new File( "file2" );

        StoreChannel channel1 = mock(StoreChannel.class);
        StoreChannel channel2 = mock(StoreChannel.class);
        when( fs.open( file1Name, "rw" ) ).thenReturn( channel1 );
        when( fs.open( file2Name, "rw" ) ).thenReturn( channel2 );


        // When
        PagedFile file1 = cache.map( file1Name, 64 );
        PagedFile file1Again = cache.map( file1Name, 64 );
        PagedFile file2 = cache.map( file2Name, 64 );
        cache.unmap( file2Name );

        // Then
        verify( fs ).open( file1Name, "rw" );
        verify( fs ).open( file2Name, "rw" );
        verify( channel2 ).close();
        verifyNoMoreInteractions( channel1, channel2, fs );

        // And When
        cache.close();

        // Then
        verify( channel1 ).close();
        verifyNoMoreInteractions( channel1, channel2, fs );
    }
}
