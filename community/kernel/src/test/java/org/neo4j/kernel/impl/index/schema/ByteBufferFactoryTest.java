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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.test.Race;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.impl.index.schema.ByteBufferFactory.HEAP_ALLOCATOR;

class ByteBufferFactoryTest
{
    @Test
    void shouldFailAcquireThreadLocalBufferIfAlreadyAcquired()
    {
        // given
        ByteBufferFactory factory = new ByteBufferFactory( () -> HEAP_ALLOCATOR, 1024 );
        factory.acquireThreadLocalBuffer();

        // when/then
        assertThrows( IllegalStateException.class, factory::acquireThreadLocalBuffer );
        factory.close();
    }

    @Test
    void shouldFailReleaseThreadLocalBufferIfNotAcquired()
    {
        // given
        ByteBufferFactory factory = new ByteBufferFactory( () -> HEAP_ALLOCATOR, 1024 );
        factory.acquireThreadLocalBuffer();
        factory.releaseThreadLocalBuffer();

        // when/then
        assertThrows( IllegalStateException.class, factory::releaseThreadLocalBuffer );
        factory.close();
    }

    @Test
    void shouldShareThreadLocalBuffersStressfully() throws Throwable
    {
        // given
        ByteBufferFactory factory = new ByteBufferFactory( () -> HEAP_ALLOCATOR, 1024 );
        Race race = new Race();
        int threads = 10;
        List<Set<ByteBuffer>> seenBuffers = new ArrayList<>();
        for ( int i = 0; i < threads; i++ )
        {
            HashSet<ByteBuffer> seen = new HashSet<>();
            seenBuffers.add( seen );
            race.addContestant( () ->
            {
                for ( int j = 0; j < 1000; j++ )
                {
                    ByteBuffer buffer = factory.acquireThreadLocalBuffer();
                    assertNotNull( buffer );
                    seen.add( buffer );
                    factory.releaseThreadLocalBuffer();
                }
            }, 1 );
        }

        // when
        race.go();

        // then
        for ( int i = 0; i < threads; i++ )
        {
            assertEquals( 1, seenBuffers.get( i ).size() );
        }
        factory.close();
    }
}
