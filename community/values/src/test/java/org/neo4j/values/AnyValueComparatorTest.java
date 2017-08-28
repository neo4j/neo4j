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
package org.neo4j.values;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.values.virtual.VirtualValueTestUtil;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValueTestUtil.edges;
import static org.neo4j.values.virtual.VirtualValueTestUtil.list;
import static org.neo4j.values.virtual.VirtualValueTestUtil.map;
import static org.neo4j.values.virtual.VirtualValueTestUtil.nodes;
import static org.neo4j.values.virtual.VirtualValues.edge;
import static org.neo4j.values.virtual.VirtualValues.edgeValue;
import static org.neo4j.values.virtual.VirtualValues.emptyMap;
import static org.neo4j.values.virtual.VirtualValues.node;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;
import static org.neo4j.values.virtual.VirtualValues.path;
import static org.neo4j.values.virtual.VirtualValues.pointCartesian;
import static org.neo4j.values.virtual.VirtualValues.pointGeographic;

public class AnyValueComparatorTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Comparator<AnyValue> comparator = AnyValues.COMPARATOR;

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
            nodeValue( 2L, stringArray( "L" ), emptyMap() ),
            node( 3L ),

            // Edge
            edge( 1L ),
            edgeValue( 2L, nodeValue( 1L, stringArray( "L" ), emptyMap() ),
                    nodeValue( 2L, stringArray( "L" ), emptyMap() ), stringValue( "type" ), emptyMap() ),
            edge( 3L ),

            // LIST AND STORABLE ARRAYS

            // List
            list(),
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
            path( nodes( 1L ), edges() ),
            path( nodes( 1L, 2L ), edges( 1L ) ),
            path( nodes( 1L, 2L, 3L ), edges( 1L, 2L ) ),
            path( nodes( 1L, 2L, 3L ), edges( 1L, 3L ) ),
            path( nodes( 1L, 2L, 3L, 4L ), edges( 1L, 3L, 4L ) ),
            path( nodes( 1L, 2L, 3L, 4L ), edges( 1L, 4L, 2L ) ),
            path( nodes( 1L, 2L, 3L ), edges( 2L, 3L ) ),
            path( nodes( 1L, 2L ), edges( 3L ) ),
            path( nodes( 2L ), edges() ),
            path( nodes( 2L, 1L ), edges( 1L ) ),
            path( nodes( 3L ), edges() ),
            path( nodes( 4L, 5L ), edges( 2L ) ),
            path( nodes( 5L, 4L ), edges( 2L ) ),

            // SCALARS AND POINTS

            // Point
            pointCartesian( -1.0, -1.0 ),
            pointCartesian( 1.0, 1.0 ),
            pointCartesian( 1.0, 2.0 ),
            pointCartesian( 2.0, 1.0 ),
            pointGeographic( -1.0, -1.0 ),
            pointGeographic( 1.0, 1.0 ),
            pointGeographic( 1.0, 2.0 ),
            pointGeographic( 2.0, 1.0 ),

            // Scalars
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
    public void shouldOrderValuesCorrectly()
    {
        List<AnyValue> values =
                Arrays.stream( objs )
                        .map( VirtualValueTestUtil::toAnyValue )
                        .collect( Collectors.toList() );

        for ( int i = 0; i < values.size(); i++ )
        {
            for ( int j = 0; j < values.size(); j++ )
            {
                AnyValue left = values.get( i );
                AnyValue right = values.get( j );

                int cmpPos = sign( i - j );
                int cmpVal = sign( compare( comparator, left, right ) );

                if ( cmpPos != cmpVal )
                {
                    throw new AssertionError( format(
                            "Comparing %s against %s does not agree with their positions in the sorted list (%d and " +
                            "%d)",
                            left, right, i, j
                    ) );
                }
            }
        }
    }

    private <T> int compare( Comparator<T> comparator, T left, T right )
    {
        int cmp1 = comparator.compare( left, right );
        int cmp2 = comparator.compare( right, left );
        if ( sign( cmp1 ) != -sign( cmp2 ) )
        {
            throw new AssertionError( format( "%s is not symmetric on %s and %s", comparator, left, right ) );
        }
        return cmp1;
    }

    private int sign( int value )
    {
        return value == 0 ? 0 : (value < 0 ? -1 : +1);
    }
}
