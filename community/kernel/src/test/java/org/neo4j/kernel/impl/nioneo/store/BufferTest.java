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
package org.neo4j.kernel.impl.nioneo.store;

import java.nio.ByteBuffer;

import org.junit.Test;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import org.neo4j.test.ReflectionUtil;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BufferTest
{
    @Test
    public void shouldDisallowAccessAfterClose() throws Exception
    {
        // Given
        Buffer buf = new Buffer( null, ByteBuffer.allocateDirect( 16 ) );

        // When
        buf.close();

        // Then
        try
        {
            buf.get();
            fail("Should not have allowed access");
        }
        catch(AssertionError e)
        {
            // ok
        }
    }

    @Test
    public void shouldCallCleanWhenOnClose() throws Exception
    {
        // Given
        final ByteBuffer directBuffer = ByteBuffer.allocateDirect( 10 );
        final Cleaner cleaner = spy( ((DirectBuffer) directBuffer).cleaner() );
        ReflectionUtil.setPrivateField( directBuffer, "cleaner", Cleaner.class, cleaner );

        // When
        new Buffer( null, directBuffer ).close();

        // Then
        verify( cleaner, times( 1 ) ).clean();
    }
}
