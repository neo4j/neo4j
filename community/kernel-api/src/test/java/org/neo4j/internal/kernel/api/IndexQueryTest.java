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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.ExistsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.NumberRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringContainsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringPrefixPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringSuffixPredicate;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexQueryTest
{
    private final int propId = 0;

    // EXISTS

    @Test
    public void testExists() throws Exception
    {
        ExistsPredicate p = IndexQuery.exists( propId );

        assertTrue( test( p, "string" ) );
        assertTrue( test( p, 1 ) );
        assertTrue( test( p, 1.0 ) );
        assertTrue( test( p, true ) );
        assertTrue( test( p, new long[]{1L} ) );

        assertFalse( test( p, null ) );
    }

    // EXACT

    @Test
    public void testExact() throws Exception
    {
        assertExactPredicate( "string" );
        assertExactPredicate( 1 );
        assertExactPredicate( 1.0 );
        assertExactPredicate( true );
        assertExactPredicate( new long[]{1L} );
    }

    private void assertExactPredicate( Object value )
    {
        ExactPredicate p = IndexQuery.exact( propId, value );

        assertTrue( test( p, value ) );

        assertFalseForOtherThings( p );
    }

    @Test
    public void testExact_ComparingBigDoublesAndLongs()
    {
        ExactPredicate p = IndexQuery.exact( propId, 9007199254740993L );

        assertFalse( test( p, 9007199254740992D ) );
    }

    // NUMERIC RANGE

    @Test
    public void testNumRange_FalseForIrrelevant()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, 13, true );

        assertFalseForOtherThings( p );
    }

    @Test
    public void testNumRange_InclusiveLowerInclusiveUpper()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, 13, true );

        assertFalse( test( p, 10 ) );
        assertTrue( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertTrue( test( p, 13 ) );
        assertFalse( test( p, 14 ) );
    }

    @Test
    public void testNumRange_ExclusiveLowerExclusiveLower()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, false, 13, false );

        assertFalse( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertFalse( test( p, 13 ) );
    }

    @Test
    public void testNumRange_InclusiveLowerExclusiveUpper()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, 13, false );

        assertFalse( test( p, 10 ) );
        assertTrue( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertFalse( test( p, 13 ) );
    }

    @Test
    public void testNumRange_ExclusiveLowerInclusiveUpper()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, false, 13, true );

        assertFalse( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertTrue( test( p, 13 ) );
        assertFalse( test( p, 14 ) );
    }

    @Test
    public void testNumRange_LowerNullValue()
    {
        NumberRangePredicate p = IndexQuery.range( propId, null, true, 13, true );

        assertTrue( test( p, 10 ) );
        assertTrue( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertTrue( test( p, 13 ) );
        assertFalse( test( p, 14 ) );
    }

    @Test
    public void testNumRange_UpperNullValue()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, null, true );

        assertFalse( test( p, 10 ) );
        assertTrue( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertTrue( test( p, 13 ) );
        assertTrue( test( p, 14 ) );
    }

    @Test
    public void testNumRange_ComparingBigDoublesAndLongs()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 9007199254740993L, true, null, true );

        assertFalse( test( p, 9007199254740992D ) );
    }

    // STRING RANGE

    @Test
    public void testStringRange_FalseForIrrelevant()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", true, "bee", true );

        assertFalseForOtherThings( p );
    }

    @Test
    public void testStringRange_InclusiveLowerInclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", true, "bee", true );

        assertFalse( test( p, "bba" ) );
        assertTrue( test( p, "bbb" ) );
        assertTrue( test( p, "bee" ) );
        assertFalse( test( p, "beea" ) );
        assertFalse( test( p, "bef" ) );
    }

    @Test
    public void testStringRange_ExclusiveLowerInclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, "bee", true );

        assertFalse( test( p, "bbb" ) );
        assertTrue( test( p, "bbba" ) );
        assertTrue( test( p, "bee" ) );
        assertFalse( test( p, "beea" ) );
    }

    @Test
    public void testStringRange_InclusiveLowerExclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", true, "bee", false );

        assertFalse( test( p, "bba" ) );
        assertTrue( test( p, "bbb" ) );
        assertTrue( test( p, "bed" ) );
        assertFalse( test( p, "bee" ) );
    }

    @Test
    public void testStringRange_ExclusiveLowerExclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, "bee", false );

        assertFalse( test( p, "bbb" ) );
        assertTrue( test( p, "bbba" ) );
        assertTrue( test( p, "bed" ) );
        assertFalse( test( p, "bee" ) );
    }

    @Test
    public void testStringRange_UpperUnbounded()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, null, false );

        assertFalse( test( p, "bbb" ) );
        assertTrue( test( p, "bbba" ) );
        assertTrue( test( p, "xxxxx" ) );
    }

    @Test
    public void testStringRange_LowerUnbounded()
    {
        StringRangePredicate p = IndexQuery.range( propId, null, false, "bee", false );

        assertTrue( test( p, "" ) );
        assertTrue( test( p, "bed" ) );
        assertFalse( test( p, "bee" ) );
    }

    // STRING PREFIX

    @Test
    public void testStringPrefix_FalseForIrrelevant()
    {
        StringPrefixPredicate p = IndexQuery.stringPrefix( propId, "dog" );

        assertFalseForOtherThings( p );
    }

    @Test
    public void testStringPrefix_SomeValues()
    {
        StringPrefixPredicate p = IndexQuery.stringPrefix( propId, "dog" );

        assertFalse( test( p, "doffington" ) );
        assertFalse( test( p, "doh, not this again!" ) );
        assertTrue( test( p, "dog" ) );
        assertTrue( test( p, "doggidog" ) );
        assertTrue( test( p, "doggidogdog" ) );
    }

    // STRING CONTAINS

    @Test
    public void testStringContains_FalseForIrrelevant()
    {
        StringContainsPredicate p = IndexQuery.stringContains( propId, "cat" );

        assertFalseForOtherThings( p );
    }

    @Test
    public void testStringContains_SomeValues()
    {
        StringContainsPredicate p = IndexQuery.stringContains( propId, "cat" );

        assertFalse( test( p, "dog" ) );
        assertFalse( test( p, "cameraman" ) );
        assertFalse( test( p, "Cat" ) );
        assertTrue( test( p, "cat" ) );
        assertTrue( test( p, "bobcat" ) );
        assertTrue( test( p, "scatman" ) );
    }

    // STRING SUFFIX

    @Test
    public void testStringSuffix_FalseForIrrelevant()
    {
        StringSuffixPredicate p = IndexQuery.stringSuffix( propId, "less" );

        assertFalseForOtherThings( p );
    }

    @Test
    public void testStringSuffix_SomeValues()
    {
        StringSuffixPredicate p = IndexQuery.stringSuffix( propId, "less" );

        assertFalse( test( p, "lesser being" ) );
        assertFalse( test( p, "make less noise please..." ) );
        assertTrue( test( p, "less" ) );
        assertTrue( test( p, "clueless" ) );
        assertTrue( test( p, "cluelessly clueless" ) );
    }

    // HELPERS

    private void assertFalseForOtherThings( IndexQuery p )
    {
        assertFalse( test( p, "other string" ) );
        assertFalse( test( p, "string1" ) );
        assertFalse( test( p, "" ) );
        assertFalse( test( p, -1 ) );
        assertFalse( test( p, -1.0 ) );
        assertFalse( test( p, false ) );
        assertFalse( test( p, new long[]{-1L} ) );
        assertFalse( test( p, null ) );
    }

    private boolean test( IndexQuery p, Object x )
    {
        return p.acceptsValue( Values.of( x ) );
    }
}
