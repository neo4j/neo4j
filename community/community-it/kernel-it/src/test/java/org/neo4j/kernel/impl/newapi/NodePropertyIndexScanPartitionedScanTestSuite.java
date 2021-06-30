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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Nested;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.Label;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.NodePropertyIndexScan;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.PropertyKey;
import org.neo4j.values.storable.Value;

import static org.assertj.core.api.Assertions.assertThat;

class NodePropertyIndexScanPartitionedScanTestSuite
        extends PropertyIndexScanPartitionedScanTestSuite<NodeValueIndexCursor>
{
    @Override
    public final NodePropertyIndexScan getFactory()
    {
        return NodePropertyIndexScan.FACTORY;
    }

    @Nested
    class WithoutData extends PropertyIndexScanPartitionedScanTestSuite.WithoutData<NodeValueIndexCursor>
    {
        WithoutData()
        {
            super( NodePropertyIndexScanPartitionedScanTestSuite.this );
        }

        @Override
        EntityIdsMatchingQuery<PropertyKeyScanQuery> setupDatabase()
        {
            final var numberOfLabels = 1;
            final var numberOfPropKeys = 2;

            final var labelId = createTags( numberOfLabels, Label.FACTORY ).get( 0 );
            final var propKeyIds = createTags( numberOfPropKeys, PropertyKey.FACTORY ).stream().mapToInt( i -> i ).toArray();
            final var labelAndPropKeyCombination = Pair.of( labelId, propKeyIds );

            final var data = emptyQueries( labelAndPropKeyCombination );
            createIndexes( createIndexPrototypes( labelAndPropKeyCombination ) );
            return data;
        }
    }

    @Nested
    class WithData extends PropertyIndexScanPartitionedScanTestSuite.WithData<NodeValueIndexCursor>
    {
        WithData()
        {
            super( NodePropertyIndexScanPartitionedScanTestSuite.this );
        }

        @Override
        EntityIdsMatchingQuery<PropertyKeyScanQuery> setupDatabase()
        {
            final var numberOfLabels = 1;
            final var numberOfPropKeys = 2;
            final var numberOfProperties = 1 << 12;

            final var labelId = createTags( numberOfLabels, Label.FACTORY ).get( 0 );
            final var propKeyIds = createTags( numberOfPropKeys, PropertyKey.FACTORY ).stream().mapToInt( i -> i ).toArray();
            final var labelAndPropKeyCombination = Pair.of( labelId, propKeyIds );

            final var data = createData( numberOfProperties, labelAndPropKeyCombination );
            createIndexes( createIndexPrototypes( labelAndPropKeyCombination ) );
            return data;
        }

        @Override
        protected EntityIdsMatchingQuery<PropertyKeyScanQuery> createData( int numberOfProperties,
                                                                           Pair<Integer,int[]> labelAndPropKeyCombination )
        {
            // given  a set of queries
            final var nodesInIndex = new EntityIdsMatchingQuery<PropertyKeyScanQuery>();

            // given  a number of properties to create
            final var propValues = random.ints( numberOfProperties ).iterator();
            var numberOfCreatedProperties = 0;
            try ( var tx = beginTx() )
            {
                final var write = tx.dataWrite();
                while ( propValues.hasNext() )
                {
                    final var labelId = labelAndPropKeyCombination.first();
                    final var propKeyIds = labelAndPropKeyCombination.other();
                    final var assignedPropValues = new Value[propKeyIds.length];

                    final var nodeId = write.nodeCreate();
                    if ( write.nodeAddLabel( nodeId, labelId ) )
                    {
                        for ( int i = 0; i < propKeyIds.length; i++ )
                        {
                            if ( propValues.hasNext() )
                            {
                                // when   properties are created
                                final var propKeyId = propKeyIds[i];
                                final var value = createValue( propValues.next() );
                                write.nodeSetProperty( nodeId, propKeyId, value );
                                numberOfCreatedProperties++;
                                assignedPropValues[i] = value;
                                // when   and tracked against queries
                                nodesInIndex.getOrCreate( new PropertyKeyScanQuery( generateIndexName( labelId, propKeyId ) ) ).add( nodeId );
                            }
                        }
                        if ( Arrays.stream( assignedPropValues ).allMatch( Objects::nonNull ) )
                        {
                            nodesInIndex.getOrCreate( new PropertyKeyScanQuery( generateIndexName( labelId, propKeyIds ) ) ).add( nodeId );
                        }
                    }
                }

                tx.commit();
            }
            catch ( KernelException e )
            {
                throw new AssertionError( "failed to create database", e );
            }

            // then   there should be some queries to match against
            assertThat( nodesInIndex.queries().size() ).as( "queries should exist" ).isGreaterThan( 0 );

            // then   and the number created should be equal to what was asked
            assertThat( numberOfCreatedProperties ).as( "node properties created" ).isEqualTo( numberOfProperties );

            return nodesInIndex;
        }
    }

    @Override
    final String generateIndexName( int tokenId, int[] propKeyIds )
    {
        return String.format( "%s[%s[%d] {%s}]", NodePropertyIndexScan.FACTORY.name(), Label.FACTORY.name(), tokenId,
                              Arrays.stream( propKeyIds ).mapToObj( String::valueOf ).collect( Collectors.joining( "," ) ) );
    }
}
