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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import org.neo4j.kernel.impl.api.LookupFilter.NumericRangeMatchPredicate;
import org.neo4j.kernel.impl.api.operations.EntityOperations;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class NumericRangeMatchPredicateTest
{

    @Test
    public void testInclusiveLowerInclusiveUpper()
    {
        NumericRangeMatchPredicate range = createRange( 11, true, 13, true );

        assertFalse( range.inRange( 10 ) );
        assertTrue( range.inRange( 11 ) );
        assertTrue( range.inRange( 12 ) );
        assertTrue( range.inRange( 13 ) );
        assertFalse( range.inRange( 14 ) );
    }

    @Test
    public void testExclusiveLowerExclusiveLower()
    {
        NumericRangeMatchPredicate range = createRange( 11, false, 13, false );

        assertFalse( range.inRange( 11 ) );
        assertTrue( range.inRange( 12 ) );
        assertFalse( range.inRange( 13 ) );
    }

    @Test
    public void testInclusiveLowerExclusiveUpper()
    {
        NumericRangeMatchPredicate range = createRange( 11, true, 13, false );

        assertFalse( range.inRange( 10 ) );
        assertTrue( range.inRange( 11 ) );
        assertTrue( range.inRange( 12 ) );
        assertFalse( range.inRange( 13 ) );
    }

    @Test
    public void testExclusiveLowerInclusiveUpper()
    {
        NumericRangeMatchPredicate range = createRange( 11, false, 13, true );

        assertFalse( range.inRange( 11 ) );
        assertTrue( range.inRange( 12 ) );
        assertTrue( range.inRange( 13 ) );
        assertFalse( range.inRange( 14 ) );
    }

    @Test
    public void testLowerNullValue()
    {
        NumericRangeMatchPredicate range = createRange( null, true, 13, true );

        assertTrue( range.inRange( 10 ) );
        assertTrue( range.inRange( 11 ) );
        assertTrue( range.inRange( 12 ) );
        assertTrue( range.inRange( 13 ) );
        assertFalse( range.inRange( 14 ) );
    }

    @Test
    public void testUpperNullValue()
    {
        NumericRangeMatchPredicate range = createRange( 11, true, null, true );

        assertFalse( range.inRange( 10 ) );
        assertTrue( range.inRange( 11 ) );
        assertTrue( range.inRange( 12 ) );
        assertTrue( range.inRange( 13 ) );
        assertTrue( range.inRange( 14 ) );
    }


    @Test
    public void testComparingBigDoublesAndLongs()
    {
        NumericRangeMatchPredicate range = createRange( 9007199254740993L, true, null, true );

        assertFalse( range.inRange( 9007199254740992D ) );
    }

    @Test
    public void testNullValue()
    {
        NumericRangeMatchPredicate range = createRange( 11, true, 13, true );

        assertFalse( range.inRange( null ) );
    }





    private NumericRangeMatchPredicate createRange( Number lower, boolean includeLower, Number upper, boolean includeUpper )
    {
        return new NumericRangeMatchPredicate( mock( EntityOperations.class ), mock( KernelStatement.class ), 11, lower,
                includeLower, upper, includeUpper );
    }

}
