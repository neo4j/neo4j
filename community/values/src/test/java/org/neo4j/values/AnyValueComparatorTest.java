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
package org.neo4j.values;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValueGroup;
import org.neo4j.values.virtual.VirtualValueTestUtil;

import static java.lang.Integer.signum;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.values.Comparison.EQUAL;
import static org.neo4j.values.Comparison.GREATER_THAN;
import static org.neo4j.values.Comparison.GREATER_THAN_AND_EQUAL;
import static org.neo4j.values.Comparison.SMALLER_THAN;
import static org.neo4j.values.Comparison.SMALLER_THAN_AND_EQUAL;
import static org.neo4j.values.Comparison.UNDEFINED;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValueTestUtil.list;
import static org.neo4j.values.virtual.VirtualValueTestUtil.map;
import static org.neo4j.values.virtual.VirtualValueTestUtil.nodes;
import static org.neo4j.values.virtual.VirtualValueTestUtil.relationships;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.concat;
import static org.neo4j.values.virtual.VirtualValues.list;
import static org.neo4j.values.virtual.VirtualValues.node;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;
import static org.neo4j.values.virtual.VirtualValues.path;
import static org.neo4j.values.virtual.VirtualValues.relationship;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;

class AnyValueComparatorTest
{
    private final AnyValueComparator comparator =
            new AnyValueComparator( Values.COMPARATOR, VirtualValueGroup::compareTo );

    private Object[] objs = new Object[]{
            // MAP LIKE TYPES

            // Map
            map(),
            map( "1", 'a' ),
            map( "1", 'b' ),
            map( "2", 'a' ),
            map( "1", map( "1", map( "1", 'a' ) ), "2", 'x' ),
            map( "1", map( "1", map( "1", 'b' ) ), "2", 'x' ),
            map( "1", 'a', "2", 'b' ),
            map( "1", 'b', "2", map() ),
            map( "1", 'b', "2", map( "10", 'a' ) ),
            map( "1", 'b', "2", map( "10", 'b' ) ),
            map( "1", 'b', "2", map( "20", 'a' ) ),
            map( "1", 'b', "2", 'a' ),

            // Node
            node( 1L ),
            nodeValue( 2L, stringArray( "L" ), EMPTY_MAP ),
            node( 3L ),

            // Edge
            relationship( 1L ),
            relationshipValue( 2L, nodeValue( 1L, stringArray( "L" ), EMPTY_MAP ),
                    nodeValue( 2L, stringArray( "L" ), EMPTY_MAP ), stringValue( "type" ), EMPTY_MAP ),
            relationship( 3L ),

            // LIST AND STORABLE ARRAYS

            // List
            VirtualValueTestUtil.list(),
            new String[]{"a"},
            new boolean[]{false},
            list( 1 ),
            list( 1, 2 ),
            list( 1, 3 ),
            list( 2, 1 ),
            new short[]{2, 3},
            list( 3 ),
            list( 3, list( 1 ) ),
            list( 3, list( 1, 2 ) ),
            list( 3, list( 2 ) ),
            list( 3, 1 ),
            new double[]{3.0, 2.0},
            list( 4, list( 1, list( 1 ) ) ),
            list( 4, list( 1, list( 2 ) ) ),
            new int[]{4, 1},

            // Path
            path( nodes( 1L ), relationships() ),
            path( nodes( 1L, 2L ), relationships( 1L ) ),
            path( nodes( 1L, 2L, 3L ), relationships( 1L, 2L ) ),
            path( nodes( 1L, 2L, 3L ), relationships( 1L, 3L ) ),
            path( nodes( 1L, 2L, 3L, 4L ), relationships( 1L, 3L, 4L ) ),
            path( nodes( 1L, 2L, 3L, 4L ), relationships( 1L, 4L, 2L ) ),
            path( nodes( 1L, 2L, 3L ), relationships( 2L, 3L ) ),
            path( nodes( 1L, 2L ), relationships( 3L ) ),
            path( nodes( 2L ), relationships() ),
            path( nodes( 2L, 1L ), relationships( 1L ) ),
            path( nodes( 3L ), relationships() ),
            path( nodes( 4L, 5L ), relationships( 2L ) ),
            path( nodes( 5L, 4L ), relationships( 2L ) ),

            // SCALARS
            pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 1.0 ),
            datetime( 2018, 2, 2, 0, 0, 0, 0, "+00:00" ),
            localDateTime( 2018, 2, 2, 0, 0, 0, 0 ),
            date( 2018, 2, 1 ),
            time( 12, 0, 0, 0, "+00:00" ),
            localTime( 0, 0, 0, 1 ),
            duration( 0, 0, 0, 0 ),
            "hello",
            true,
            1L,
            Math.PI,
            Short.MAX_VALUE,
            Double.NaN,

            // OTHER
            null,
    };

    @Test
    void shouldOrderValuesCorrectly()
    {
        List<AnyValue> values = Arrays.stream( objs )
                .map( VirtualValueTestUtil::toAnyValue )
                .collect( Collectors.toList() );

        for ( int i = 0; i < values.size(); i++ )
        {
            for ( int j = 0; j < values.size(); j++ )
            {
                AnyValue left = values.get( i );
                AnyValue right = values.get( j );

                int cmpPos = signum( i - j );
                int cmpVal = signum( compare( comparator, left, right ) );
                assertEquals( cmpPos, cmpVal,
                        format( "Comparing %s against %s does not agree with their positions in the sorted list (%d " +
                                "and %d)",
                                left, right, i, j ) );
                Comparison comparison = comparator.ternaryCompare( left, right );
                switch ( comparison )
                {
                case GREATER_THAN:
                case EQUAL:
                case SMALLER_THAN:
                    assertEquals( cmpVal, comparison.value() );
                    break;
                //The rest of the values doesn't have defined order
                case GREATER_THAN_AND_EQUAL:
                case SMALLER_THAN_AND_EQUAL:
                case UNDEFINED:
                default:
                }
            }
        }
    }

    @Test
    void shouldHandleListCompareWithIteration()
    {
        assertThat( comparator.compare( list( intValue( 1 ), intValue( 2 ) ), list( intValue( 2 ), intValue( 1 ) ) ),
                lessThan( 0 ) );
        assertThat( comparator.compare( concat( list( intValue( 1 ) ), list( intValue( 2 ) ) ),
                list( intValue( 2 ), intValue( 1 ) ) ), lessThan( 0 ) );
    }

    @Test
    void shouldTernaryCompareNaNs()
    {
        assertTernaryCompare( Values.NaN, Values.E, UNDEFINED );
        assertTernaryCompare( Values.E, Values.NaN, UNDEFINED );
        assertTernaryCompare( Values.NaN, Values.NaN, UNDEFINED );
    }

    @Test
    void shouldHandleNoValueCorrectlyInTernaryCompare()
    {
        assertTernaryCompare( NO_VALUE, stringValue( "foo" ), UNDEFINED );
        assertTernaryCompare( stringValue( "foo" ), NO_VALUE, UNDEFINED );
        assertTernaryCompare( stringValue( "42" ), intValue( 42 ), UNDEFINED );
        assertTernaryCompare( NO_VALUE, NO_VALUE, UNDEFINED );
    }

    @Test
    void shouldTernaryCompareLists()
    {
        assertTernaryCompare( EMPTY_LIST, EMPTY_LIST, EQUAL );
        assertTernaryCompare( EMPTY_LIST, Values.EMPTY_BYTE_ARRAY, EQUAL );
        assertTernaryCompare( EMPTY_LIST, NO_VALUE, UNDEFINED );
        assertTernaryCompare( EMPTY_LIST, list( "foo" ), SMALLER_THAN );
        assertTernaryCompare( EMPTY_LIST, stringArray( "foo" ), SMALLER_THAN );
        assertTernaryCompare( list( "foo" ), stringArray( "foo" ), EQUAL );
        assertTernaryCompare( list( stringArray( "foo" ) ), list( stringArray( "foo" ) ), EQUAL );
        assertTernaryCompare( list( list( "foo" ) ), list( list( "foo" ) ), EQUAL );
        assertTernaryCompare( list( stringValue( "foo" ) ), intValue( 42 ), UNDEFINED );
        assertTernaryCompare( list( stringValue( "foo" ) ), list( intValue( 42 ) ), UNDEFINED );

        ListValue listReference = list( stringValue( "foo" ), NO_VALUE, intValue( 42 ) );
        assertTernaryCompare( listReference, list( stringValue( "foo" ), NO_VALUE, intValue( 42 ) ), UNDEFINED );
        //make sure we don't do reference equal check
        assertTernaryCompare( listReference, listReference, UNDEFINED );

        assertTernaryCompare( list( stringValue( "foo" ) ), list( stringValue( "foo" ), intValue( 42 ), NO_VALUE ),
                SMALLER_THAN );
        assertTernaryCompare( list( stringValue( "foo" ), intValue( 42 ), NO_VALUE ), list( stringValue( "foo" ) ),
                GREATER_THAN );
    }

    @Test
    void shouldTernaryCompareMaps()
    {
        assertTernaryCompare( EMPTY_MAP, EMPTY_MAP, EQUAL );
        assertTernaryCompare( EMPTY_MAP, map( "foo", 42 ), SMALLER_THAN );
        assertTernaryCompare( map( "foo", 42 ), map( "foo", 42 ), EQUAL );
        assertTernaryCompare( map( "foo", 42 ), map( "foo", 43 ), SMALLER_THAN );
        assertTernaryCompare( map( "bar", 42 ), map( "foo", 42 ), SMALLER_THAN );
        assertTernaryCompare( map( "foo", NO_VALUE ), map( "foo", 42 ), UNDEFINED );
        assertTernaryCompare( map( "bar", NO_VALUE ), map( "foo", 42 ), SMALLER_THAN );
        assertTernaryCompare( map( "foo", list( 1, 2, 3 ) ), map( "foo", list( 1, 2, 3 ) ), EQUAL );
        assertTernaryCompare( map( "foo", list( 1, 5, 3 ) ), map( "foo", list( 1, 2, 3 ) ), GREATER_THAN );
        VirtualValue mapWithNoValue = map( "foo", NO_VALUE );
        assertTernaryCompare( mapWithNoValue, map( "foo", NO_VALUE ), UNDEFINED );
        //make sure we don't short cut on referential equality
        assertTernaryCompare( mapWithNoValue, mapWithNoValue, UNDEFINED );
        assertTernaryCompare( mapWithNoValue, stringValue( "foo" ), UNDEFINED );
    }

    @Test
    void shouldTernaryCompareNodes()
    {
        assertTernaryCompare( nodeValue( 42, stringArray( "L" ), EMPTY_MAP ),
                nodeValue( 42, stringArray( "L" ), EMPTY_MAP ), EQUAL );
        assertTernaryCompare( nodeValue( 42, stringArray( "L" ), EMPTY_MAP ),
                nodeValue( 43, stringArray( "L" ), EMPTY_MAP ), SMALLER_THAN );
        assertTernaryCompare( node( 42 ), nodeValue( 42, stringArray( "L" ), EMPTY_MAP ), EQUAL );
        assertTernaryCompare( nodeValue( 42, stringArray( "L" ), EMPTY_MAP ), intValue( 42 ), UNDEFINED );
        MapValue propMap = map( "foo", "bar" );
        assertTernaryCompare( nodeValue( 42, stringArray( "L" ), propMap ), propMap, UNDEFINED );
    }

    @Test
    void shouldTernaryCompareRelationships()
    {
        NodeValue start = nodeValue( 42, stringArray( "L" ), EMPTY_MAP );
        NodeValue end = nodeValue( 43, stringArray( "L" ), EMPTY_MAP );
        MapValue propMap = map( "foo", "bar" );
        RelationshipValue rel1 = relationshipValue( 42, start, end, stringValue( "R" ), propMap );
        RelationshipValue rel2 = relationshipValue( 43, start, end, stringValue( "R" ), propMap );

        assertTernaryCompare( rel1, rel1, EQUAL );
        assertTernaryCompare( rel1, rel2, SMALLER_THAN );
        assertTernaryCompare( rel1, relationship( rel1.id() ), EQUAL );
        assertTernaryCompare( rel1, intValue( 42 ), UNDEFINED );
        assertTernaryCompare( rel1, propMap, UNDEFINED );
    }

    @Test
    void shouldTernaryComparePaths()
    {
        assertTernaryCompare(
                path( nodes( 1, 2, 3 ), relationships( 10, 11 ) ),
                path( nodes( 1, 2, 3 ), relationships( 10, 11 ) ), EQUAL );
        assertTernaryCompare(
                path( nodes( 1, 2, 3 ), relationships( 10, 11 ) ),
                path( nodes( 1, 2 ), relationships( 10 ) ), GREATER_THAN );
        assertTernaryCompare(
                path( nodes( 1, 2, 3 ), relationships( 10, 11 ) ),
                list( nodes( 1, 2, 3 ) ), UNDEFINED );
    }

    @Test
    void shouldTernaryComparePoints()
    {
        PointValue pointValue1 = pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 1.0 );
        PointValue pointValue2 = pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 2.0 );

        assertTernaryCompare( pointValue1, pointValue1, EQUAL );
        assertTernaryCompare( pointValue1, pointValue2, SMALLER_THAN_AND_EQUAL );
        assertTernaryCompare( pointValue1, Values.doubleArray( new double[]{1.0, 1.0} ), UNDEFINED );
        assertTernaryCompare( pointValue1, pointValue( CoordinateReferenceSystem.WGS84, 1.0, 1.0 ), UNDEFINED );
    }

    @Test
    void shouldTernaryCompareTemporalValues()
    {
        AnyValue[] temporalValues = {datetime( 2018, 2, 2, 0, 0, 0, 0, "+00:00" ),
                localDateTime( 2018, 2, 2, 0, 0, 0, 0 ),
                date( 2018, 2, 1 ),
                time( 12, 0, 0, 0, "+00:00" ),
                localTime( 0, 0, 0, 1 )};
        for ( AnyValue value1 : temporalValues )
        {
            for ( AnyValue value2 : temporalValues )
            {
                Comparison expected = value1 == value2 ? EQUAL : UNDEFINED;
                assertTernaryCompare( value1, value2, expected );
            }
        }
    }

    @Test
    void shouldTernaryCompareDurationValues()
    {
        DurationValue duration = duration( 0, 0, 0, 0 );

        assertTernaryCompare( duration, duration, EQUAL );
        assertTernaryCompare( duration, duration( 0, 0, 0, 0 ), EQUAL );
        assertTernaryCompare( duration, duration( 1, 0, 0, 0 ), UNDEFINED );
        assertTernaryCompare( duration,  localTime( 0, 0, 0, 1 ), UNDEFINED );
    }

    private void assertTernaryCompare( AnyValue a, AnyValue b, Comparison expected )
    {
        Comparison comparison = comparator.ternaryCompare( a, b );
        assertThat( comparison, equalTo(  expected ));
        switch ( expected )
        {
        case GREATER_THAN:
            assertThat( comparator.ternaryCompare( b, a ), equalTo( SMALLER_THAN ));
            break;
        case EQUAL:
            assertThat( comparator.ternaryCompare( b, a ), equalTo( EQUAL ));
            break;
        case SMALLER_THAN:
            assertThat( comparator.ternaryCompare( b, a ), equalTo( GREATER_THAN ));
            break;
        case UNDEFINED:
            assertThat( comparator.ternaryCompare( b, a ), equalTo( UNDEFINED ));
            break;
        case GREATER_THAN_AND_EQUAL:
            assertThat( comparator.ternaryCompare( b, a ), equalTo( SMALLER_THAN_AND_EQUAL ));
            break;
        case SMALLER_THAN_AND_EQUAL:
            assertThat( comparator.ternaryCompare( b, a ), equalTo( GREATER_THAN_AND_EQUAL ));
            break;
        default:
            fail( "Was not expecting " + expected );
        }
    }

    private int compare( AnyValueComparator comparator, AnyValue left, AnyValue right )
    {
        int cmp1 = comparator.compare( left, right );
        int cmp2 = comparator.compare( right, left );
        assertEquals( signum( cmp1 ), -signum( cmp2 ),
                format( "%s is not symmetric on %s and %s", comparator, left, right ) );
        return cmp1;
    }
}
