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

import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.ExistsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.GeometryRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.NumberRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringContainsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringPrefixPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringSuffixPredicate;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexQueryTest
{
    private final int propId = 0;

    // EXISTS

    @Test
    void testExists()
    {
        ExistsPredicate p = IndexQuery.exists( propId );

        assertTrue( test( p, "string" ) );
        assertTrue( test( p, 1 ) );
        assertTrue( test( p, 1.0 ) );
        assertTrue( test( p, true ) );
        assertTrue( test( p, new long[]{1L} ) );
        assertTrue( test( p, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) ) );

        assertFalse( test( p, null ) );
    }

    // EXACT

    @Test
    void testExact()
    {
        assertExactPredicate( "string" );
        assertExactPredicate( 1 );
        assertExactPredicate( 1.0 );
        assertExactPredicate( true );
        assertExactPredicate( new long[]{1L} );
        assertExactPredicate( Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) );
    }

    private void assertExactPredicate( Object value )
    {
        ExactPredicate p = IndexQuery.exact( propId, value );

        assertTrue( test( p, value ) );

        assertFalseForOtherThings( p );
    }

    @Test
    void testExact_ComparingBigDoublesAndLongs()
    {
        ExactPredicate p = IndexQuery.exact( propId, 9007199254740993L );

        assertFalse( test( p, 9007199254740992D ) );
    }

    // NUMERIC RANGE

    @Test
    void testNumRange_FalseForIrrelevant()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, 13, true );

        assertFalseForOtherThings( p );
    }

    @Test
    void testNumRange_InclusiveLowerInclusiveUpper()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, 13, true );

        assertFalse( test( p, 10 ) );
        assertTrue( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertTrue( test( p, 13 ) );
        assertFalse( test( p, 14 ) );
    }

    @Test
    void testNumRange_ExclusiveLowerExclusiveLower()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, false, 13, false );

        assertFalse( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertFalse( test( p, 13 ) );
    }

    @Test
    void testNumRange_InclusiveLowerExclusiveUpper()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, 13, false );

        assertFalse( test( p, 10 ) );
        assertTrue( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertFalse( test( p, 13 ) );
    }

    @Test
    void testNumRange_ExclusiveLowerInclusiveUpper()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, false, 13, true );

        assertFalse( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertTrue( test( p, 13 ) );
        assertFalse( test( p, 14 ) );
    }

    @Test
    void testNumRange_LowerNullValue()
    {
        NumberRangePredicate p = IndexQuery.range( propId, null, true, 13, true );

        assertTrue( test( p, 10 ) );
        assertTrue( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertTrue( test( p, 13 ) );
        assertFalse( test( p, 14 ) );
    }

    @Test
    void testNumRange_UpperNullValue()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 11, true, null, true );

        assertFalse( test( p, 10 ) );
        assertTrue( test( p, 11 ) );
        assertTrue( test( p, 12 ) );
        assertTrue( test( p, 13 ) );
        assertTrue( test( p, 14 ) );
    }

    @Test
    void testNumRange_ComparingBigDoublesAndLongs()
    {
        NumberRangePredicate p = IndexQuery.range( propId, 9007199254740993L, true, null, true );

        assertFalse( test( p, 9007199254740992D ) );
    }

    // STRING RANGE

    @Test
    void testStringRange_FalseForIrrelevant()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", true, "bee", true );

        assertFalseForOtherThings( p );
    }

    @Test
    void testStringRange_InclusiveLowerInclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", true, "bee", true );

        assertFalse( test( p, "bba" ) );
        assertTrue( test( p, "bbb" ) );
        assertTrue( test( p, "bee" ) );
        assertFalse( test( p, "beea" ) );
        assertFalse( test( p, "bef" ) );
    }

    @Test
    void testStringRange_ExclusiveLowerInclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, "bee", true );

        assertFalse( test( p, "bbb" ) );
        assertTrue( test( p, "bbba" ) );
        assertTrue( test( p, "bee" ) );
        assertFalse( test( p, "beea" ) );
    }

    @Test
    void testStringRange_InclusiveLowerExclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", true, "bee", false );

        assertFalse( test( p, "bba" ) );
        assertTrue( test( p, "bbb" ) );
        assertTrue( test( p, "bed" ) );
        assertFalse( test( p, "bee" ) );
    }

    @Test
    void testStringRange_ExclusiveLowerExclusiveUpper()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, "bee", false );

        assertFalse( test( p, "bbb" ) );
        assertTrue( test( p, "bbba" ) );
        assertTrue( test( p, "bed" ) );
        assertFalse( test( p, "bee" ) );
    }

    @Test
    void testStringRange_UpperUnbounded()
    {
        StringRangePredicate p = IndexQuery.range( propId, "bbb", false, null, false );

        assertFalse( test( p, "bbb" ) );
        assertTrue( test( p, "bbba" ) );
        assertTrue( test( p, "xxxxx" ) );
    }

    @Test
    void testStringRange_LowerUnbounded()
    {
        StringRangePredicate p = IndexQuery.range( propId, null, false, "bee", false );

        assertTrue( test( p, "" ) );
        assertTrue( test( p, "bed" ) );
        assertFalse( test( p, "bee" ) );
    }

    // GEOMETRY RANGE

    private PointValue gps1 = Values.pointValue( CoordinateReferenceSystem.WGS84, -12.6, -56.7 );
    private PointValue gps2 = Values.pointValue( CoordinateReferenceSystem.WGS84, -12.6, -55.7 );
    private PointValue gps3 = Values.pointValue( CoordinateReferenceSystem.WGS84, -11.0, -55 );
    private PointValue gps4 = Values.pointValue( CoordinateReferenceSystem.WGS84, 0, 0 );
    private PointValue gps5 = Values.pointValue( CoordinateReferenceSystem.WGS84, 14.6, 56.7 );
    private PointValue gps6 = Values.pointValue( CoordinateReferenceSystem.WGS84, 14.6, 58.7 );
    private PointValue gps7 = Values.pointValue( CoordinateReferenceSystem.WGS84, 15.6, 59.7 );
    private PointValue car1 = Values.pointValue( CoordinateReferenceSystem.Cartesian, 0, 0 );
    private PointValue car2 = Values.pointValue( CoordinateReferenceSystem.Cartesian, 2, 2 );
    private PointValue car3 = Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 1, 2, 3 );
    private PointValue car4 = Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 2, 3, 4 );
    private PointValue gps1_3d = Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 12.6, 56.8, 100.0 );
    private PointValue gps2_3d = Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 12.8, 56.9, 200.0 );

    //TODO: Also insert points which can't be compared e.g. Cartesian and (-100, 100)

    @Test
    void testGeometryRange_FalseForIrrelevant()
    {
        GeometryRangePredicate p = IndexQuery.range( propId, gps2, true, gps5, true );

        assertFalseForOtherThings( p );
    }

    @Test
    void testGeometryRange_InclusiveLowerInclusiveUpper()
    {
        GeometryRangePredicate p = IndexQuery.range( propId, gps2, true, gps5, true );

        assertFalse( test( p, gps1 ) );
        assertTrue( test( p, gps2 ) );
        assertTrue( test( p, gps5 ) );
        assertFalse( test( p, gps6 ) );
        assertFalse( test( p, gps7 ) );
        assertFalse( test( p, car1 ) );
        assertFalse( test( p, car2 ) );
        assertFalse( test( p, car3 ) );
        assertFalse( test( p, gps1_3d ) );
    }

    @Test
    void testGeometryRange_ExclusiveLowerInclusiveUpper()
    {
        GeometryRangePredicate p = IndexQuery.range( propId, gps2, false, gps5, true );

        assertFalse( test( p, gps2 ) );
        assertTrue( test( p, gps3 ) );
        assertTrue( test( p, gps5 ) );
        assertFalse( test( p, gps6 ) );
        assertFalse( test( p, car1 ) );
        assertFalse( test( p, car2 ) );
        assertFalse( test( p, car3 ) );
        assertFalse( test( p, gps1_3d ) );
    }

    @Test
    void testGeometryRange_InclusiveLowerExclusiveUpper()
    {
        GeometryRangePredicate p = IndexQuery.range( propId, gps2, true, gps5, false );

        assertFalse( test( p, gps1 ) );
        assertTrue( test( p, gps2 ) );
        assertTrue( test( p, gps3 ) );
        assertFalse( test( p, gps5 ) );
        assertFalse( test( p, car1 ) );
        assertFalse( test( p, car2 ) );
        assertFalse( test( p, car3 ) );
        assertFalse( test( p, gps1_3d ) );
    }

    @Test
    void testGeometryRange_ExclusiveLowerExclusiveUpper()
    {
        GeometryRangePredicate p = IndexQuery.range( propId, gps2, false, gps5, false );

        assertFalse( test( p, gps2 ) );
        assertTrue( test( p, gps3 ) );
        assertTrue( test( p, gps4 ) );
        assertFalse( test( p, gps5 ) );
        assertFalse( test( p, car1 ) );
        assertFalse( test( p, car2 ) );
        assertFalse( test( p, car3 ) );
        assertFalse( test( p, gps1_3d ) );
    }

    @Test
    void testGeometryRange_UpperUnbounded()
    {
        GeometryRangePredicate p = IndexQuery.range( propId, gps2, false, null, false );

        assertFalse( test( p, gps2 ) );
        assertTrue( test( p, gps3 ) );
        assertTrue( test( p, gps7 ) );
        assertFalse( test( p, car1 ) );
        assertFalse( test( p, car2 ) );
        assertFalse( test( p, car3 ) );
        assertFalse( test( p, gps1_3d ) );
    }

    @Test
    void testGeometryRange_LowerUnbounded()
    {
        GeometryRangePredicate p = IndexQuery.range( propId, null, false, gps5, false );

        assertTrue( test( p, gps1 ) );
        assertTrue( test( p, gps3 ) );
        assertFalse( test( p, gps5 ) );
        assertFalse( test( p, car1 ) );
        assertFalse( test( p, car2 ) );
        assertFalse( test( p, car3 ) );
        assertFalse( test( p, gps1_3d ) );
    }

    @Test
    public void testGeometryRange_Cartesian()
    {
        GeometryRangePredicate p = IndexQuery.range( propId, car1, false, car2, true );

        assertFalse( test( p, gps1 ) );
        assertFalse( test( p, gps3 ) );
        assertFalse( test( p, gps5 ) );
        assertFalse( test( p, car1 ) );
        assertTrue( test( p, car2 ) );
        assertFalse( test( p, car3 ) );
        assertFalse( test( p, car4 ) );
        assertFalse( test( p, gps1_3d ) );
        assertFalse( test( p, gps2_3d ) );
    }

    @Test
    public void testGeometryRange_Cartesian3D()
    {
        GeometryRangePredicate p = IndexQuery.range( propId, car3, true, car4, true );

        assertFalse( test( p, gps1 ) );
        assertFalse( test( p, gps3 ) );
        assertFalse( test( p, gps5 ) );
        assertFalse( test( p, car1 ) );
        assertFalse( test( p, car2 ) );
        assertTrue( test( p, car3 ) );
        assertTrue( test( p, car4 ) );
        assertFalse( test( p, gps1_3d ) );
        assertFalse( test( p, gps2_3d ) );
    }

    @Test
    public void testGeometryRange_WGS84_3D()
    {
        GeometryRangePredicate p = IndexQuery.range( propId, gps1_3d, true, gps2_3d, true );

        assertFalse( test( p, gps1 ) );
        assertFalse( test( p, gps3 ) );
        assertFalse( test( p, gps5 ) );
        assertFalse( test( p, car1 ) );
        assertFalse( test( p, car2 ) );
        assertFalse( test( p, car3 ) );
        assertFalse( test( p, car4 ) );
        assertTrue( test( p, gps1_3d ) );
        assertTrue( test( p, gps2_3d ) );
    }

    // STRING PREFIX

    @Test
    void testStringPrefix_FalseForIrrelevant()
    {
        StringPrefixPredicate p = IndexQuery.stringPrefix( propId, "dog" );

        assertFalseForOtherThings( p );
    }

    @Test
    void testStringPrefix_SomeValues()
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
    void testStringContains_FalseForIrrelevant()
    {
        StringContainsPredicate p = IndexQuery.stringContains( propId, "cat" );

        assertFalseForOtherThings( p );
    }

    @Test
    void testStringContains_SomeValues()
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
    void testStringSuffix_FalseForIrrelevant()
    {
        StringSuffixPredicate p = IndexQuery.stringSuffix( propId, "less" );

        assertFalseForOtherThings( p );
    }

    @Test
    void testStringSuffix_SomeValues()
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
