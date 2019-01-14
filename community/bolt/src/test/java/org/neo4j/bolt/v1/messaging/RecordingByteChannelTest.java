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
package org.neo4j.bolt.v1.messaging;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class RecordingByteChannelTest
{

    @Test
    public void shouldBeAbleToWriteToThenReadFromChannel()
    {
        // Given
        RecordingByteChannel channel = new RecordingByteChannel();

        // When
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        channel.write( ByteBuffer.wrap( data ) );
        ByteBuffer buffer = ByteBuffer.allocate( 10 );
        int bytesRead = channel.read( buffer );

        // Then
        assertThat( bytesRead, equalTo( 5 ) );
        assertThat( buffer.array(), equalTo( new byte[]{1, 2, 3, 4, 5, 0, 0, 0, 0, 0} ) );

    }

}
