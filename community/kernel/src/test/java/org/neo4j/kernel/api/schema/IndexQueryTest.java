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

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class IndexQueryTest
{
    private int propId = 0;

    // EXISTS

    @Test
    public void testExists() throws Exception
    {
        ExistsPredicate p = IndexQuery.exists( propId );

        assertTrue( p.test( "string" ) );
        assertTrue( p.test( 1 ) );
        assertTrue( p.test( 1.0 ) );
        assertTrue( p.test( true ) );
        assertTrue( p.test( new long[]{1L} ) );

        assertFalse( p.test( null ) );
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

        assertTrue( p.test( value ) );

        assertFalseForOtherThings( p );
    }

    @Test
    public void testExact_ComparingBigDoublesAndLongs()
    {
        ExactPredicate p = IndexQuery.exact( propId, 9007199254740993L );

        Assert.assertFalse( p.test( 9007199254740992D ) );
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

        Assert.assertFalse( p.test( 10 ) );
        Assert.assertTrue( p.test( 11 ) );
        Assert.assertTrue( p.test( 12 ) );
        Assert.assertTrue( p.test( 13 ) );
        Assert.assertFalse( p.test( 14 ) );
    }

    @Test
    public void testNumRange_ExclusiveLowerExclusiveLower()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, false, 13, false );

        Assert.assertFalse( p.test( 11 ) );
        Assert.assertTrue( p.test( 12 ) );
        Assert.assertFalse( p.test( 13 ) );
    }

    @Test
    public void testNumRange_InclusiveLowerExclusiveUpper()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, 13, false );

        Assert.assertFalse( p.test( 10 ) );
        Assert.assertTrue( p.test( 11 ) );
        Assert.assertTrue( p.test( 12 ) );
        Assert.assertFalse( p.test( 13 ) );
    }

    @Test
    public void testNumRange_ExclusiveLowerInclusiveUpper()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, false, 13, true );

        Assert.assertFalse( p.test( 11 ) );
        Assert.assertTrue( p.test( 12 ) );
        Assert.assertTrue( p.test( 13 ) );
        Assert.assertFalse( p.test( 14 ) );
    }

    @Test
    public void testNumRange_LowerNullValue()
    {
        NumberRangePredicate p = IndexQuery.range( propId, null, true, 13, true );

        Assert.assertTrue( p.test( 10 ) );
        Assert.assertTrue( p.test( 11 ) );
        Assert.assertTrue( p.test( 12 ) );
        Assert.assertTrue( p.test( 13 ) );
        Assert.assertFalse( p.test( 14 ) );
    }

    @Test
    public void testNumRange_UpperNullValue()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, null, true );

        Assert.assertFalse( p.test( 10 ) );
        Assert.assertTrue( p.test( 11 ) );
        Assert.assertTrue( p.test( 12 ) );
        Assert.assertTrue( p.test( 13 ) );
        Assert.assertTrue( p.test( 14 ) );
    }

    @Test
    public void testNumRange_ComparingBigDoublesAndLongs()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 9007199254740993L, true, null, true );

        Assert.assertFalse( p.test( 9007199254740992D ) );
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

        Assert.assertFalse( p.test( "bba" ) );
        Assert.assertTrue( p.test( "bbb" ) );
        Assert.assertTrue( p.test( "bee" ) );
        Assert.assertFalse( p.test( "beea" ) );
        Assert.assertFalse( p.test( "bef" ) );
    }

    @Test
    public void testStringRange_ExclusiveLowerInclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, "bee", true );

        Assert.assertFalse( p.test( "bbb" ) );
        Assert.assertTrue( p.test( "bbba" ) );
        Assert.assertTrue( p.test( "bee" ) );
        Assert.assertFalse( p.test( "beea" ) );
    }

    @Test
    public void testStringRange_InclusiveLowerExclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", true, "bee", false );

        Assert.assertFalse( p.test( "bba" ) );
        Assert.assertTrue( p.test( "bbb" ) );
        Assert.assertTrue( p.test( "bed" ) );
        Assert.assertFalse( p.test( "bee" ) );
    }

    @Test
    public void testStringRange_ExclusiveLowerExclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, "bee", false );

        Assert.assertFalse( p.test( "bbb" ) );
        Assert.assertTrue( p.test( "bbba" ) );
        Assert.assertTrue( p.test( "bed" ) );
        Assert.assertFalse( p.test( "bee" ) );
    }

    @Test
    public void testStringRange_UpperUnbounded()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, null, false );

        Assert.assertFalse( p.test( "bbb" ) );
        Assert.assertTrue( p.test( "bbba" ) );
        Assert.assertTrue( p.test( "xxxxx" ) );
    }

    @Test
    public void testStringRange_LowerUnbounded()
    {
        StringRangePredicate p = IndexQuery.range( propId, null, false, "bee", false );

        Assert.assertTrue( p.test( "" ) );
        Assert.assertTrue( p.test( "bed" ) );
        Assert.assertFalse( p.test( "bee" ) );
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

        Assert.assertFalse( p.test( "doffington" ) );
        Assert.assertFalse( p.test( "doh, not this again!" ) );
        Assert.assertTrue( p.test( "dog" ) );
        Assert.assertTrue( p.test( "doggidog" ) );
        Assert.assertTrue( p.test( "doggidogdog" ) );
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

        Assert.assertFalse( p.test( "dog" ) );
        Assert.assertFalse( p.test( "cameraman" ) );
        Assert.assertTrue( p.test( "cat" ) );
        Assert.assertTrue( p.test( "bobcat" ) );
        Assert.assertTrue( p.test( "scatman" ) );
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

        Assert.assertFalse( p.test( "lesser being" ) );
        Assert.assertFalse( p.test( "make less noise please..." ) );
        Assert.assertTrue( p.test( "less" ) );
        Assert.assertTrue( p.test( "clueless" ) );
        Assert.assertTrue( p.test( "cluelessly clueless" ) );
    }

    // HELPERS

    private void assertFalseForOtherThings( IndexQuery p )
    {
        assertFalse( p.test( "other string" ) );
        assertFalse( p.test( "string1" ) );
        assertFalse( p.test( "" ) );
        assertFalse( p.test( -1 ) );
        assertFalse( p.test( -1.0 ) );
        assertFalse( p.test( false ) );
        assertFalse( p.test( new long[]{-1L} ) );
        assertFalse( p.test( null ) );
    }

}
