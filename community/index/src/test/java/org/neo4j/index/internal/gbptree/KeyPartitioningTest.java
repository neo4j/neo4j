/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.index.internal.gbptree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

import static java.lang.Math.abs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith( RandomExtension.class )
class KeyPartitioningTest
{
    @Inject
    private RandomSupport random;

    @Test
    void shouldPartitionEvenly()
    {
        // given
        Layout<PartitionKey,?> layout = layout();
        int numberOfKeys = random.nextInt( 50, 200 );
        SortedSet<PartitionKey> allKeys = keys( numberOfKeys );
        KeyPartitioning<PartitionKey> partitioning = new KeyPartitioning<>( layout );

        // when
        int from = random.nextInt( numberOfKeys - 1 );
        int to = random.nextInt( from, numberOfKeys );
        int numberOfPartitions = from == to ? 1 : random.nextInt( 1, to - from );

        List<PartitionKey> partitionEdges = partitioning.partition( allKeys, new PartitionKey( from ), new PartitionKey( to ), numberOfPartitions );

        // then verify that the partitions have no seams in between them, that they cover the whole requested range and are fairly evenly distributed
        assertEquals( numberOfPartitions, partitionEdges.size() - 1 );
        assertEquals( from, partitionEdges.get( 0 ).value );
        assertEquals( to, partitionEdges.get( partitionEdges.size() - 1 ).value );
        int diff = diff( partitionEdges, 0 );
        for ( int i = 1; i < partitionEdges.size() - 2; i++ )
        {
            assertTrue( abs( diff - diff( partitionEdges, i ) ) <= 1 );
        }
    }

    private static int diff( List<PartitionKey> partitionEdges, int partition )
    {
        assertTrue( partition < partitionEdges.size() - 2 );
        return partitionEdges.get( partition ).value - partitionEdges.get( partition + 1 ).value;
    }

    private static Layout<PartitionKey,?> layout()
    {
        Layout<PartitionKey,?> layout = mock( Layout.class );
        when( layout.newKey() ).thenAnswer( invocationOnMock -> new PartitionKey() );
        when( layout.copyKey( any(), any() ) ).thenAnswer( invocationOnMock ->
        {
            invocationOnMock.getArgument( 1, PartitionKey.class ).value = invocationOnMock.getArgument( 0, PartitionKey.class ).value;
            return null;
        } );
        when( layout.compare( any(), any() ) ).thenAnswer( invocationOnMock -> Integer.compare( invocationOnMock.getArgument( 0, PartitionKey.class ).value,
                invocationOnMock.getArgument( 1, PartitionKey.class ).value ) );
        return layout;
    }

    private static SortedSet<PartitionKey> keys( int count )
    {
        return IntStream.range( 0, count )
                        .mapToObj( PartitionKey::new )
                        .collect( Collectors.toCollection( () -> new TreeSet<>( layout() ) ) );
    }

    private static class PartitionKey
    {
        int value;

        PartitionKey()
        {
            this( 0 );
        }

        PartitionKey( int value )
        {
            this.value = value;
        }

        @Override
        public String toString()
        {
            return String.valueOf( value );
        }
    }
}
