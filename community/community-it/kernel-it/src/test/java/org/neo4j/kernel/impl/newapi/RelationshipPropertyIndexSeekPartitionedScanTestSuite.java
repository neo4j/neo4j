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

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.RelationshipPropertyIndexSeek;
import org.neo4j.values.storable.Value;

import static org.assertj.core.api.Assertions.assertThat;

class RelationshipPropertyIndexSeekPartitionedScanTestSuite
        extends PropertyIndexSeekPartitionedScanTestSuite<RelationshipValueIndexCursor>
{
    RelationshipPropertyIndexSeekPartitionedScanTestSuite( IndexType index )
    {
        super( index );
    }

    @Override
    public final RelationshipPropertyIndexSeek getFactory()
    {
        return RelationshipPropertyIndexSeek.FACTORY;
    }

    @Nested
    class WithoutData extends PropertyIndexSeekPartitionedScanTestSuite.WithoutData<RelationshipValueIndexCursor>
    {
        WithoutData()
        {
            super( RelationshipPropertyIndexSeekPartitionedScanTestSuite.this );
        }

        @Override
        EntityIdsMatchingQuery<PropertyKeySeekQuery> setupDatabase()
        {
            final var numberOfRelTypes = 1;
            final var numberOfPropKeys = 2;

            final var relTypeId = createTags( numberOfRelTypes, factory.getTokenFactory() ).get( 0 );
            final var propKeyIds = createTags( numberOfPropKeys, factory.getPropKeyFactory() ).stream().mapToInt( i -> i ).toArray();
            final var relTypeAndPropKeyCombination = Pair.of( relTypeId, propKeyIds );

            createIndexes( createIndexPrototypes( relTypeAndPropKeyCombination ) );
            return emptyQueries( relTypeAndPropKeyCombination );
        }
    }

    @Nested
    class WithData extends PropertyIndexSeekPartitionedScanTestSuite.WithData<RelationshipValueIndexCursor>
    {
        WithData()
        {
            super( RelationshipPropertyIndexSeekPartitionedScanTestSuite.this );
        }

        @Override
        EntityIdsMatchingQuery<PropertyKeySeekQuery> setupDatabase()
        {
            final var numberOfRelTypes = 1;
            final var numberOfPropKeys = 2;
            final var numberOfProperties = 1 << 12;
            ratioForExactQuery = 0.002;

            final var relTypeId = createTags( numberOfRelTypes, factory.getTokenFactory() ).get( 0 );
            final var propKeyIds = createTags( numberOfPropKeys, factory.getPropKeyFactory() ).stream().mapToInt( i -> i ).toArray();
            final var relTypeAndPropKeyCombination = Pair.of( relTypeId, propKeyIds );

            createIndexes( createIndexPrototypes( relTypeAndPropKeyCombination ) );
            return createData( numberOfProperties, relTypeAndPropKeyCombination );
        }

        @Override
        protected EntityIdsMatchingQuery<PropertyKeySeekQuery> createData( int numberOfProperties,
                                                                           Pair<Integer,int[]> relTypeAndPropKeyCombination )
        {
            // given  a set of queries
            final var tracking = new TrackEntityIdsMatchingQuery();

            // given  a number of properties to create
            final var propValues = random.ints( numberOfProperties ).iterator();
            var numberOfCreatedProperties = 0;
            try ( var tx = beginTx() )
            {
                final var write = tx.dataWrite();
                while ( propValues.hasNext() )
                {
                    final var relTypeId = relTypeAndPropKeyCombination.first();
                    final var propKeyIds = relTypeAndPropKeyCombination.other();
                    final var assignedPropValues = new Value[propKeyIds.length];

                    final var relId = write.relationshipCreate( write.nodeCreate(), relTypeId, write.nodeCreate() );
                    for ( int i = 0; i < propKeyIds.length; i++ )
                    {
                        if ( propValues.hasNext() )
                        {
                            // when   properties are created
                            final var propKeyId = propKeyIds[i];
                            final var value = createValue( propValues.next() );
                            write.relationshipSetProperty( relId, propKeyId, value );
                            numberOfCreatedProperties++;
                            assignedPropValues[i] = value;
                            // when   and tracked against queries
                            final var index = factory.getIndex( tx, relTypeId, propKeyId );
                            tracking.generateAndTrack( relId, index, propKeyId, value, shouldIncludeExactQuery() );
                        }
                    }
                    final var index = factory.getIndex( tx, relTypeId, propKeyIds );
                    tracking.generateAndTrack( relId, index, propKeyIds, assignedPropValues, shouldIncludeExactQuery() );
                }

                tx.commit();
            }
            catch ( KernelException e )
            {
                throw new AssertionError( "failed to create database", e );
            }

            // then   there should be some queries to match against
            assertThat( tracking.get().queries().size() ).as( "queries should exist" ).isGreaterThan( 0 );

            // then   and the number created should be equal to what was asked
            assertThat( numberOfCreatedProperties ).as( "node properties created" ).isEqualTo( numberOfProperties );

            return tracking.get();
        }
    }
}
