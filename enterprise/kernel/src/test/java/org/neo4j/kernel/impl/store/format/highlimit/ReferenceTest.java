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
package org.neo4j.kernel.impl.store.format.highlimit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import javax.annotation.Resource;

import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( RandomExtension.class )
public class ReferenceTest
{
    /**
     * The current scheme only allows us to use 58 bits for a reference. Adhere to that limit here.
     */
    private static final long MASK = numberOfBits( Reference.MAX_BITS );
    private static final int PAGE_SIZE = 100;

    @Resource
    public RandomRule random;
    private final StubPageCursor cursor = new StubPageCursor( 0, PAGE_SIZE );

    @Test
    public void shouldEncodeRandomLongs()
    {
        for ( int i = 0; i < 100_000_000; i++ )
        {
            long reference = limit( random.nextLong() );
            assertDecodedMatchesEncoded( reference );
        }
    }

    @Test
    public void relativeReferenceConvertion()
    {
        long basis = 0xBABE;
        long absoluteReference = 0xCAFEBABE;

        long relative = Reference.toRelative( absoluteReference, basis );
        assertEquals( 0xCAFE0000, relative, "Should be equal to difference of reference and base reference" );

        long absoluteCandidate = Reference.toAbsolute( relative, basis );
        assertEquals( absoluteReference, absoluteCandidate, "Converted reference should be equal to initial value" );
    }

    private static long numberOfBits( int count )
    {
        long result = 0;
        for ( int i = 0; i < count; i++ )
        {
            result = (result << 1) | 1;
        }
        return result;
    }

    private static long limit( long reference )
    {
        boolean positive = true;
        if ( reference < 0 )
        {
            positive = false;
            reference = ~reference;
        }

        reference &= MASK;

        if ( !positive )
        {
            reference = ~reference;
        }
        return reference;
    }

    private void assertDecodedMatchesEncoded( long reference )
    {
        cursor.setOffset( 0 );
        Reference.encode( reference, cursor );

        cursor.setOffset( 0 );
        long read = Reference.decode( cursor );
        assertEquals( reference, read );
    }
}
