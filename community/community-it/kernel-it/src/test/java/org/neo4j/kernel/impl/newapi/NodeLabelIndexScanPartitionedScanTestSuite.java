/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.Tokens.Suppliers.UUID.LABEL;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.junit.jupiter.api.Nested;
import org.neo4j.common.EntityType;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.NodeLabelIndexScan;

abstract class NodeLabelIndexScanPartitionedScanTestSuite
        extends TokenIndexScanPartitionedScanTestSuite<NodeLabelIndexCursor> {
    @Override
    public final NodeLabelIndexScan getFactory() {
        return NodeLabelIndexScan.FACTORY;
    }

    @Nested
    class WithoutData extends TokenIndexScanPartitionedScanTestSuite.WithoutData<NodeLabelIndexCursor> {
        WithoutData() {
            super(NodeLabelIndexScanPartitionedScanTestSuite.this);
        }

        @Override
        Queries<TokenScanQuery> setupDatabase() {
            final var numberOfLabels = 3;

            final var labelIds = createTokens(numberOfLabels, LABEL);
            return emptyQueries(EntityType.NODE, labelIds);
        }
    }

    @Nested
    class WithData extends TokenIndexScanPartitionedScanTestSuite.WithData<NodeLabelIndexCursor> {
        WithData() {
            super(NodeLabelIndexScanPartitionedScanTestSuite.this);
        }

        @Override
        Queries<TokenScanQuery> setupDatabase() {
            final var numberOfNodes = 1L << 18;

            final var labelRanges = tokenRangesFromTokenId(LABEL, createTokenRanges(numberOfNodes));
            return createData(numberOfNodes, labelRanges);
        }

        @Override
        Queries<TokenScanQuery> createData(long numberOfNodes, SortedMap<Integer, List<Range>> labelRanges) {
            // given  a number of nodes to create
            final var nodesWithLabelId = new EntityIdsMatchingQuery<TokenScanQuery>();
            final var indexName = getTokenIndexName(EntityType.NODE);
            labelRanges
                    .keySet()
                    .forEach(labelId ->
                            nodesWithLabelId.getOrCreate(new TokenScanQuery(indexName, new TokenPredicate(labelId))));

            try (var tx = beginTx()) {
                final var write = tx.dataWrite();
                for (int i = 0; i < numberOfNodes; i++) {
                    // when   nodes are created
                    final var nodeId = write.nodeCreate();
                    final var nodeCreated = i;
                    final var potentialLabelIds = labelRanges.entrySet().stream()
                            .filter(entry -> entry.getValue().stream().anyMatch(range -> range.contains(nodeCreated)))
                            .mapToInt(Map.Entry::getKey)
                            .toArray();
                    if (potentialLabelIds.length > 0) {
                        final var labelId = random.among(potentialLabelIds);
                        if (write.nodeAddLabel(nodeId, labelId)) {
                            // when   and tracked against a query
                            nodesWithLabelId
                                    .getOrCreate(new TokenScanQuery(indexName, new TokenPredicate(labelId)))
                                    .add(nodeId);
                        }
                    }
                }

                tx.commit();
            } catch (Exception e) {
                throw new AssertionError("failed to create database", e);
            }

            try (var tx = beginTx()) {
                // then   and the number created should be equal to what was asked
                assertThat(tx.dataRead().nodesGetCount()).as("nodes created").isEqualTo(numberOfNodes);
            } catch (Exception e) {
                throw new AssertionError("failed to count number of nodes", e);
            }

            return new Queries<>(nodesWithLabelId);
        }
    }
}
