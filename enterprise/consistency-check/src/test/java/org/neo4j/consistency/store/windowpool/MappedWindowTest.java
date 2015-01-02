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

import static java.nio.ByteBuffer.allocateDirect;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.Buffer;

public class MappedWindowTest
{
    @Test
    public void shouldProvideBufferWithCorrectOffset() throws Exception
    {
        // given
        ByteBuffer byteBuffer = allocateDirect( 150 );
        byteBuffer.putInt( 15*5, 42 );
        MappedWindow window = new MappedWindow( 10, 15, 100, (MappedByteBuffer) byteBuffer );

        // when
        Buffer buffer = window.getOffsettedBuffer( 105 );

        // then
        assertEquals( 42, buffer.getInt() );
    }

    @Test
    public void shouldClose() throws Exception
    {
        // given


        // when

        // then
    }
}
