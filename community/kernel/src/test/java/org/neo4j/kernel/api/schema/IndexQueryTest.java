/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.schema;

import org.junit.Assert;
import org.junit.Test;

import org.neo4j.kernel.api.schema.IndexQuery.ExactPredicate;
import org.neo4j.kernel.api.schema.IndexQuery.ExistsPredicate;
import org.neo4j.kernel.api.schema.IndexQuery.NumberRangePredicate;
import org.neo4j.kernel.api.schema.IndexQuery.StringContainsPredicate;
import org.neo4j.kernel.api.schema.IndexQuery.StringPrefixPredicate;
import org.neo4j.kernel.api.schema.IndexQuery.StringRangePredicate;
import org.neo4j.kernel.api.schema.IndexQuery.StringSuffixPredicate;
import org.neo4j.values.storable.Values;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

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

        Assert.assertFalse( test( p, 9007199254740992D ) );
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

        Assert.assertFalse( test( p, 10 ) );
        Assert.assertTrue( test( p, 11 ) );
        Assert.assertTrue( test( p, 12 ) );
        Assert.assertTrue( test( p, 13 ) );
        Assert.assertFalse( test( p, 14 ) );
    }

    @Test
    public void testNumRange_ExclusiveLowerExclusiveLower()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, false, 13, false );

        Assert.assertFalse( test( p, 11 ) );
        Assert.assertTrue( test( p, 12 ) );
        Assert.assertFalse( test( p, 13 ) );
    }

    @Test
    public void testNumRange_InclusiveLowerExclusiveUpper()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, 13, false );

        Assert.assertFalse( test( p, 10 ) );
        Assert.assertTrue( test( p, 11 ) );
        Assert.assertTrue( test( p, 12 ) );
        Assert.assertFalse( test( p, 13 ) );
    }

    @Test
    public void testNumRange_ExclusiveLowerInclusiveUpper()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, false, 13, true );

        Assert.assertFalse( test( p, 11 ) );
        Assert.assertTrue( test( p, 12 ) );
        Assert.assertTrue( test( p, 13 ) );
        Assert.assertFalse( test( p, 14 ) );
    }

    @Test
    public void testNumRange_LowerNullValue()
    {
        NumberRangePredicate p = IndexQuery.range( propId, null, true, 13, true );

        Assert.assertTrue( test( p, 10 ) );
        Assert.assertTrue( test( p, 11 ) );
        Assert.assertTrue( test( p, 12 ) );
        Assert.assertTrue( test( p, 13 ) );
        Assert.assertFalse( test( p, 14 ) );
    }

    @Test
    public void testNumRange_UpperNullValue()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, null, true );

        Assert.assertFalse( test( p, 10 ) );
        Assert.assertTrue( test( p, 11 ) );
        Assert.assertTrue( test( p, 12 ) );
        Assert.assertTrue( test( p, 13 ) );
        Assert.assertTrue( test( p, 14 ) );
    }

    @Test
    public void testNumRange_ComparingBigDoublesAndLongs()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 9007199254740993L, true, null, true );

        Assert.assertFalse( test( p, 9007199254740992D ) );
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

        Assert.assertFalse( test( p, "bba" ) );
        Assert.assertTrue( test( p, "bbb" ) );
        Assert.assertTrue( test( p, "bee" ) );
        Assert.assertFalse( test( p, "beea" ) );
        Assert.assertFalse( test( p, "bef" ) );
    }

    @Test
    public void testStringRange_ExclusiveLowerInclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, "bee", true );

        Assert.assertFalse( test( p, "bbb" ) );
        Assert.assertTrue( test( p, "bbba" ) );
        Assert.assertTrue( test( p, "bee" ) );
        Assert.assertFalse( test( p, "beea" ) );
    }

    @Test
    public void testStringRange_InclusiveLowerExclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", true, "bee", false );

        Assert.assertFalse( test( p, "bba" ) );
        Assert.assertTrue( test( p, "bbb" ) );
        Assert.assertTrue( test( p, "bed" ) );
        Assert.assertFalse( test( p, "bee" ) );
    }

    @Test
    public void testStringRange_ExclusiveLowerExclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, "bee", false );

        Assert.assertFalse( test( p, "bbb" ) );
        Assert.assertTrue( test( p, "bbba" ) );
        Assert.assertTrue( test( p, "bed" ) );
        Assert.assertFalse( test( p, "bee" ) );
    }

    @Test
    public void testStringRange_UpperUnbounded()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, null, false );

        Assert.assertFalse( test( p, "bbb" ) );
        Assert.assertTrue( test( p, "bbba" ) );
        Assert.assertTrue( test( p, "xxxxx" ) );
    }

    @Test
    public void testStringRange_LowerUnbounded()
    {
        StringRangePredicate p = IndexQuery.range( propId, null, false, "bee", false );

        Assert.assertTrue( test( p, "" ) );
        Assert.assertTrue( test( p, "bed" ) );
        Assert.assertFalse( test( p, "bee" ) );
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

        Assert.assertFalse( test( p, "doffington" ) );
        Assert.assertFalse( test( p, "doh, not this again!" ) );
        Assert.assertTrue( test( p, "dog" ) );
        Assert.assertTrue( test( p, "doggidog" ) );
        Assert.assertTrue( test( p, "doggidogdog" ) );
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

        Assert.assertFalse( test( p, "dog" ) );
        Assert.assertFalse( test( p, "cameraman" ) );
        Assert.assertTrue( test( p, "cat" ) );
        Assert.assertTrue( test( p, "bobcat" ) );
        Assert.assertTrue( test( p, "scatman" ) );
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

        Assert.assertFalse( test( p, "lesser being" ) );
        Assert.assertFalse( test( p, "make less noise please..." ) );
        Assert.assertTrue( test( p, "less" ) );
        Assert.assertTrue( test( p, "clueless" ) );
        Assert.assertTrue( test( p, "cluelessly clueless" ) );
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
        return p.test( Values.of( x ) );
    }
}
