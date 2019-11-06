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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

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
    private RandomRule random;

    @Test
    void shouldPartitionEvenly()
    {
        // given
        Layout<PartitionKey,?> layout = layout();
        int numberOfKeys = random.nextInt( 50, 200 );
        List<PartitionKey> allKeys = keys( numberOfKeys );
        KeyPartitioning<PartitionKey> partitioning = new KeyPartitioning<>( layout );

        // when
        int from = random.nextInt( numberOfKeys - 1 );
        int to = random.nextInt( from, numberOfKeys );
        int numberOfPartitions = from == to ? 1 : random.nextInt( 1, to - from );
        List<Pair<PartitionKey,PartitionKey>> partitions =
                partitioning.partition( allKeys, new PartitionKey( from ), new PartitionKey( to ), numberOfPartitions );

        // then verify that the partitions have no seams in between them, that they cover the whole requested range and are fairly evenly distributed
        assertEquals( numberOfPartitions, partitions.size() );
        assertEquals( from, partitions.get( 0 ).getLeft().value );
        assertEquals( to, partitions.get( partitions.size() - 1 ).getRight().value );
        int diff = diff( partitions.get( 0 ) );
        for ( int i = 1; i < partitions.size(); i++ )
        {
            Pair<PartitionKey,PartitionKey> prev = partitions.get( i - 1 );
            Pair<PartitionKey,PartitionKey> current = partitions.get( i );
            assertEquals( prev.getRight().value, current.getLeft().value );
            assertTrue( abs( diff - diff( current ) ) <= 1 );
        }
    }

    private static int diff( Pair<PartitionKey,PartitionKey> partition )
    {
        return partition.getRight().value - partition.getLeft().value;
    }

    private Layout<PartitionKey,?> layout()
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

    private List<PartitionKey> keys( int count )
    {
        List<PartitionKey> keys = new ArrayList<>();
        for ( int i = 0; i < count; i++ )
        {
            keys.add( new PartitionKey( i ) );
        }
        return keys;
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
