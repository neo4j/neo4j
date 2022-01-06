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

import java.util.List;
import java.util.stream.LongStream;

import org.neo4j.common.EntityType;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.RelationshipType;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.RelationshipTypeIndexScan;

import static org.assertj.core.api.Assertions.assertThat;

abstract class RelationshipTypeIndexScanPartitionedScanTestSuite
        extends TokenIndexScanPartitionedScanTestSuite<RelationshipTypeIndexCursor>
{
    @Override
    public final RelationshipTypeIndexScan getFactory()
    {
        return RelationshipTypeIndexScan.FACTORY;
    }

    @Nested
    class WithoutData extends TokenIndexScanPartitionedScanTestSuite.WithoutData<RelationshipTypeIndexCursor>
    {
        WithoutData()
        {
            super( RelationshipTypeIndexScanPartitionedScanTestSuite.this );
        }

        @Override
        Queries<TokenScanQuery> setupDatabase()
        {
            final var numberOfRelTypes = 3;

            final var relTypeIds = createTags( numberOfRelTypes, RelationshipType.FACTORY );
            return emptyQueries( EntityType.RELATIONSHIP, relTypeIds );
        }
    }

    @Nested
    class WithData extends TokenIndexScanPartitionedScanTestSuite.WithData<RelationshipTypeIndexCursor>
    {
        WithData()
        {
            super( RelationshipTypeIndexScanPartitionedScanTestSuite.this );
        }

        @Override
        Queries<TokenScanQuery> setupDatabase()
        {
            final var numberOfRelTypes = 3;
            final var numberOfRelationships = 100_000;

            final var relTypeIds = createTags( numberOfRelTypes, RelationshipType.FACTORY );
            return createData( numberOfRelationships, relTypeIds );
        }

        @Override
        Queries<TokenScanQuery> createData( int numberOfRelationships, List<Integer> relTypeIds )
        {
            // given  a number of relationships to create
            final var relsWithRelTypeId = new EntityIdsMatchingQuery<TokenScanQuery>();
            final var indexName = getTokenIndexName( EntityType.RELATIONSHIP );

            final var selfRelRatio = 0.1f;
            final var numberOfNodes = numberOfRelationships / 10;
            // dense node threshold ~50; pick 64 to be safe
            final var denseNodeNumberOfRels = 64;
            // at least 1% dense nodes
            final var numberOfDenseNodes = numberOfRelationships / denseNodeNumberOfRels / 100;

            try ( var tx = beginTx() )
            {
                final var write = tx.dataWrite();
                // create finite nodes to create graph from
                // to ensure a connected network of relationships
                final var nodeIds = LongStream.generate( write::nodeCreate ).limit( numberOfNodes ).toArray();

                var relationshipsToCreate = numberOfRelationships;
                // when   relationships are created
                // when   and tracked against a query

                // dense nodes
                for ( int i = 0; relationshipsToCreate > 0 && i < numberOfDenseNodes; i++ )
                {
                    final var denseNodeId = random.among( nodeIds );
                    for ( int j = 0; relationshipsToCreate > 0 && j < denseNodeNumberOfRels; relationshipsToCreate--, j++ )
                    {
                        final var otherNodeId = random.nextFloat() < selfRelRatio ? denseNodeId : random.among( nodeIds );
                        final var forward = random.nextBoolean();
                        final var sourceNodeId = forward ? denseNodeId : otherNodeId;
                        final var targetNodeId = forward ? otherNodeId : denseNodeId;
                        final var relTypeId = random.among( relTypeIds );
                        final var relId = write.relationshipCreate( sourceNodeId, relTypeId, targetNodeId );
                        relsWithRelTypeId.getOrCreate( new TokenScanQuery( indexName, new TokenPredicate( relTypeId ) ) ).add( relId );
                    }
                }

                // rest regular singular relationships
                for ( ; relationshipsToCreate > 0; relationshipsToCreate-- )
                {
                    final var relTypeId = random.among( relTypeIds );
                    final var sourceNodeId = random.among( nodeIds );
                    final var targetNodeId = random.nextFloat() < selfRelRatio ? sourceNodeId : random.among( nodeIds );
                    final var relId = write.relationshipCreate( sourceNodeId, relTypeId, targetNodeId );
                    relsWithRelTypeId.getOrCreate( new TokenScanQuery( indexName, new TokenPredicate( relTypeId ) ) ).add( relId );
                }

                tx.commit();
            }
            catch ( Exception e )
            {
                throw new AssertionError( "failed to create database", e );
            }

            var numberOfCreatedRels = 0;
            for ( final var entry : relsWithRelTypeId )
            {
                numberOfCreatedRels += entry.getValue().size();
            }
            // then   and the number created should be equal to what was asked
            assertThat( numberOfCreatedRels ).as( "relationships created" ).isEqualTo( numberOfRelationships );

            return new Queries<>( relsWithRelTypeId );
        }
    }
}
