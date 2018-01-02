/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.collection.primitive.hopscotch;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;

public class JumpingSequencePutTest
{
    @Test
    public void shouldHandlePathologicalSequenceCase() throws Exception
    {
        // Given
        PrimitiveLongSet set = Primitive.longSet();
        Sequence seqGen = new Sequence();

        // When
        for ( int i = 0; i < 10000; i++ )
        {
            set.add( seqGen.next() );
        }

        // Then it should not have run out of RAM
    }

    /**
     * To be frank, I don't understand the intricacies of how this works, but
     * this is a cut-out version of the sequence generator that triggered the original bug.
     * The gist is that it generates sequences of ids that "jump" to a much higher number
     * every one hundred ids or so.
     */
    private class Sequence
    {
        private static final int sizePerJump = 100;
        private final AtomicLong nextId = new AtomicLong();
        private int leftToNextJump = sizePerJump / 2;
        private long highBits = 0;

        public long next()
        {
            long result = tryNextId();
            if ( --leftToNextJump == 0 )
            {
                leftToNextJump = sizePerJump;
                nextId.set( (0xFFFFFFFFL | (highBits++ << 32)) - sizePerJump / 2 + 1 );
            }
            return result;
        }

        private long tryNextId()
        {
            long result = nextId.getAndIncrement();
            if ( result == 0xFFFFFFFFL ) // 4294967295L
            {
                result = nextId.getAndIncrement();
                leftToNextJump--;
            }
            return result;
        }
    }

}
