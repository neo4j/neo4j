/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.aligned;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.format.aligned.Reference;
import org.neo4j.test.RandomRule;

import static org.junit.Assert.assertEquals;

import static org.neo4j.kernel.impl.store.format.aligned.Reference.PAGE_CURSOR_ADAPTER;

public class ReferenceTest
{
    public final @Rule RandomRule random = new RandomRule();
    private final ByteBuffer buffer = ByteBuffer.allocateDirect( 100 );
    private final StubPageCursor cursor = new StubPageCursor( 0, buffer );

    @Test
    public void shouldEncodeRandomLongs() throws Exception
    {
        // WHEN/THEN
        long mask = numberOfBits( 58 );
        for ( int i = 0; i < 100_000_000; i++ )
        {
            long reference = limit( random.nextLong(), mask );
            assertDecodedMatchesEncoded( reference );
        }
    }

    private long numberOfBits( int count )
    {
        long result = 0;
        for ( int i = 0; i < count; i++ )
        {
            result = (result << 1) | 1;
        }
        return result;
    }

    /**
     * The current scheme only allows us to use 58 bits for a reference. Adhere to that limit here.
     */
    private long limit( long reference, long mask )
    {
        boolean positive = true;
        if ( reference < 0 )
        {
            positive = false;
            reference = ~reference;
        }

        reference &= mask;

        if ( !positive )
        {
            reference = ~reference;
        }
        return reference;
    }

    private void assertDecodedMatchesEncoded( long reference ) throws IOException
    {
        cursor.setOffset( 0 );
        Reference.encode( reference, cursor, PAGE_CURSOR_ADAPTER );

        cursor.setOffset( 0 );
        long read = Reference.decode( cursor, PAGE_CURSOR_ADAPTER );
        assertEquals( reference, read );
    }
}
