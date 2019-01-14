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
package org.neo4j.kernel.impl.newapi;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith( Parameterized.class )
public class SortedMergeJoinTest
{
    @Parameterized.Parameters()
    public static Iterable<Object[]> data()
    {
        return Arrays.asList(new Object[][] {
                { IndexOrder.ASCENDING }, { IndexOrder.DESCENDING }
        });
    }

    @Parameterized.Parameter
    public IndexOrder indexOrder;

    @Test
    public void shouldWorkWithEmptyLists()
    {
        assertThatItWorksOneWay( Collections.emptyList(), Collections.emptyList() );
    }

    @Test
    public void shouldWorkWithAList()
    {
        assertThatItWorks( Arrays.asList(
                                   node( 1L, "a" ),
                                   node( 3L, "aa" ),
                                   node( 5L, "c" ),
                                   node( 7L, "g" ) ),
                           Collections.emptyList() );
    }

    @Test
    public void shouldWorkWith2Lists()
    {
        assertThatItWorks( Arrays.asList(
                                   node( 1L, "a" ),
                                   node( 3L, "aa" ),
                                   node( 5L, "c" ),
                                   node( 7L, "g" ) ),
                           Arrays.asList(
                                   node( 2L, "b" ),
                                   node( 4L, "ba" ),
                                   node( 6L, "ca" ),
                                   node( 8L, "d" ) ) );
    }

    @Test
    public void shouldWorkWithSameElements()
    {
        assertThatItWorks( Arrays.asList(
                node( 1L, "a" ),
                node( 3L, "b" ),
                node( 5L, "c" ) ),
                           Arrays.asList(
                                   node( 2L, "aa" ),
                                   node( 3L, "b" ),
                                   node( 6L, "ca" ) ) );
    }

    @Test
    public void shouldWorkWithCompositeValues()
    {
        assertThatItWorks( Arrays.asList(
                                   node( 1L, "a", "a" ),
                                   node( 3L, "b", "a" ),
                                   node( 5L, "b", "b" ),
                                   node( 7L, "c", "d" ) ),
                           Arrays.asList(
                                   node( 2L, "a", "b" ),
                                   node( 5L, "b", "b" ),
                                   node( 6L, "c", "e" ) ) );
    }

    private void assertThatItWorks( List<NodeWithPropertyValues> listA, List<NodeWithPropertyValues> listB )
    {
        assertThatItWorksOneWay( listA, listB );
        assertThatItWorksOneWay( listB, listA );
    }

    private void assertThatItWorksOneWay( List<NodeWithPropertyValues> listA, List<NodeWithPropertyValues> listB )
    {
        SortedMergeJoin sortedMergeJoin = new SortedMergeJoin();
        sortedMergeJoin.initialize( indexOrder );

        Comparator<NodeWithPropertyValues> comparator = indexOrder == IndexOrder.ASCENDING ?
                ( a, b ) -> ValueTuple.COMPARATOR.compare( ValueTuple.of( a.getValues() ), ValueTuple.of( b.getValues() ) ) :
                ( a, b ) -> ValueTuple.COMPARATOR.compare( ValueTuple.of( b.getValues() ), ValueTuple.of( a.getValues() ) );

        listA.sort( comparator );
        listB.sort( comparator );

        List<NodeWithPropertyValues> result = process( sortedMergeJoin, listA.iterator(), listB.iterator() );

        List<NodeWithPropertyValues> expected = new ArrayList<>();
        expected.addAll( listA );
        expected.addAll( listB );
        expected.sort( comparator );

        assertThat( result, equalTo( expected ) );
    }

    private List<NodeWithPropertyValues> process( SortedMergeJoin sortedMergeJoin,
                                                  Iterator<NodeWithPropertyValues> iteratorA,
                                                  Iterator<NodeWithPropertyValues> iteratorB )
    {
        Collector collector = new Collector();
        while ( !collector.done )
        {
            if ( iteratorA.hasNext() && sortedMergeJoin.needsA() )
            {
                NodeWithPropertyValues a = iteratorA.next();
                sortedMergeJoin.setA( a.getNodeId(), a.getValues() );
            }
            if ( iteratorB.hasNext() && sortedMergeJoin.needsB() )
            {
                NodeWithPropertyValues b = iteratorB.next();
                sortedMergeJoin.setB( b.getNodeId(), b.getValues() );
            }

            sortedMergeJoin.next( collector );
        }
        return collector.result;
    }

    private NodeWithPropertyValues node( long id, Object... values )
    {
        return new NodeWithPropertyValues( id, Stream.of( values ).map( Values::of ).toArray( Value[]::new ) );
    }

    class Collector implements SortedMergeJoin.Sink
    {
        final List<NodeWithPropertyValues> result = new ArrayList<>();
        boolean done;

        @Override
        public void acceptSortedMergeJoin( long nodeId, Value[] values )
        {
            if ( nodeId == -1 )
            {
                done = true;
            }
            else
            {
                result.add( new NodeWithPropertyValues( nodeId, values ) );
            }
        }
    }
}
