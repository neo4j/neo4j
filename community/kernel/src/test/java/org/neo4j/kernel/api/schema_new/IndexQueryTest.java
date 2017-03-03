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
package org.neo4j.kernel.api.schema_new;

import org.junit.Assert;
import org.junit.Test;

import org.neo4j.kernel.api.schema_new.IndexQuery.ExactPredicate;
import org.neo4j.kernel.api.schema_new.IndexQuery.ExistsPredicate;
import org.neo4j.kernel.api.schema_new.IndexQuery.NumberRangePredicate;

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
