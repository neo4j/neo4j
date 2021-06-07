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

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.Label;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.NodeLabelIndex;

import static org.assertj.core.api.Assertions.assertThat;

class NodeLabelIndexPartitionedScanTestSuite
        extends TokenIndexPartitionedScanTestSuite<NodeLabelIndexCursor>
{
    @Override
    public final NodeLabelIndex getFactory()
    {
        return NodeLabelIndex.FACTORY;
    }

    @Nested
    class WithoutData extends TokenIndexPartitionedScanTestSuite.WithoutData<NodeLabelIndexCursor>
    {
        WithoutData()
        {
            super( NodeLabelIndexPartitionedScanTestSuite.this );
        }

        @Override
        EntityIdsMatchingScanQuery<TokenScanQuery> setupDatabase()
        {
            final var numberOfLabels = 3;

            final var labelIds = createTags( numberOfLabels, Label.FACTORY );
            return emptyScanQueries( EntityType.NODE, labelIds );
        }
    }

    @Nested
    class WithData extends TokenIndexPartitionedScanTestSuite.WithData<NodeLabelIndexCursor>
    {
        WithData()
        {
            super( NodeLabelIndexPartitionedScanTestSuite.this );
        }

        @Override
        EntityIdsMatchingScanQuery<TokenScanQuery> setupDatabase()
        {
            final var numberOfLabels = 3;
            final var numberOfNodes = 100_000;

            final var labelIds = createTags( numberOfLabels, Label.FACTORY );
            return createData( numberOfNodes, labelIds );
        }

        @Override
        EntityIdsMatchingScanQuery<TokenScanQuery> createData( int numberOfNodes, List<Integer> labelIds )
        {
            // given  a number of nodes to create
            final var nodesWithLabelId = new EntityIdsMatchingScanQuery<TokenScanQuery>();
            final var indexName = getTokenIndexName( EntityType.NODE );
            try ( var tx = beginTx() )
            {
                final var write = tx.dataWrite();
                for ( int i = 0; i < numberOfNodes; i++ )
                {
                    // when   nodes are created
                    final var nodeId = write.nodeCreate();
                    final var labelId = labelIds.get( i % labelIds.size() );
                    if ( write.nodeAddLabel( nodeId, labelId ) )
                    {
                        // when   and tracked against a query
                        nodesWithLabelId.getOrCreate( new TokenScanQuery( indexName, new TokenPredicate( labelId ) ) ).add( nodeId );
                    }
                }

                tx.commit();
            }
            catch ( KernelException e )
            {
                throw new AssertionError( "failed to create database", e );
            }

            // then   there should be some queries to match against
            assertThat( nodesWithLabelId.scanQueries().size() ).as( "queries should exist" ).isGreaterThan( 0 );

            var numberOfCreatedNodes = 0;
            for ( var entry : nodesWithLabelId )
            {
                numberOfCreatedNodes += entry.getValue().size();
            }
            // then   and the number created should be equal to what was asked
            assertThat( numberOfCreatedNodes ).as( "nodes created" ).isEqualTo( numberOfNodes );

            return nodesWithLabelId;
        }
    }
}
