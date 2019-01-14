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
package org.neo4j.io.fs;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


public class StoreFileChannelTest
{
    @Test
    public void shouldHandlePartialWrites() throws Exception
    {
        // Given
        FileChannel mockChannel = mock(FileChannel.class);
        when(mockChannel.write( any(ByteBuffer.class), anyLong() )).thenReturn( 4 );

        ByteBuffer buffer = ByteBuffer.wrap( "Hello, world!".getBytes( StandardCharsets.UTF_8 ) );

        StoreFileChannel channel = new StoreFileChannel( mockChannel );

        // When
        channel.writeAll( buffer, 20 );

        // Then
        verify( mockChannel ).write( buffer, 20 );
        verify( mockChannel ).write( buffer, 24 );
        verify( mockChannel ).write( buffer, 28 );
        verify( mockChannel ).write( buffer, 32 );
        verifyNoMoreInteractions( mockChannel );
    }
}
