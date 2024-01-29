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
import static org.neo4j.test.Tokens.Suppliers.UUID.RELATIONSHIP_TYPE;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.neo4j.common.EntityType;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.RelationshipTypeIndexScan;

abstract class RelationshipTypeIndexScanPartitionedScanTestSuite
        extends TokenIndexScanPartitionedScanTestSuite<RelationshipTypeIndexCursor> {
    @Override
    public final RelationshipTypeIndexScan getFactory() {
        return RelationshipTypeIndexScan.FACTORY;
    }

    @Nested
    class WithoutData extends TokenIndexScanPartitionedScanTestSuite.WithoutData<RelationshipTypeIndexCursor> {
        WithoutData() {
            super(RelationshipTypeIndexScanPartitionedScanTestSuite.this);
        }

        @Override
        Queries<TokenScanQuery> setupDatabase() {
            final var numberOfRelTypes = 3;

            final var relTypeIds = createTokens(numberOfRelTypes, RELATIONSHIP_TYPE);
            return emptyQueries(EntityType.RELATIONSHIP, relTypeIds);
        }
    }

    @Nested
    class WithData extends TokenIndexScanPartitionedScanTestSuite.WithData<RelationshipTypeIndexCursor> {
        private int defaultRelType;

        WithData() {
            super(RelationshipTypeIndexScanPartitionedScanTestSuite.this);
        }

        @Override
        Queries<TokenScanQuery> setupDatabase() {
            final var numberOfRelationships = 1L << 18;

            final var relTypeRanges =
                    tokenRangesFromTokenId(RELATIONSHIP_TYPE, createTokenRanges(numberOfRelationships));
            defaultRelType = createToken(RELATIONSHIP_TYPE);
            return createData(numberOfRelationships, relTypeRanges);
        }

        @Override
        Queries<TokenScanQuery> createData(long numberOfRelationships, SortedMap<Integer, List<Range>> relTypeRanges) {
            // given  a number of relationships to create
            final var relsWithRelTypeId = new EntityIdsMatchingQuery<TokenScanQuery>();
            final var indexName = getTokenIndexName(EntityType.RELATIONSHIP);
            relTypeRanges
                    .keySet()
                    .forEach(relTypeId -> relsWithRelTypeId.getOrCreate(
                            new TokenScanQuery(indexName, new TokenPredicate(relTypeId))));

            final var numberOfNodes = numberOfRelationships / 10;
            // ~10% loops
            final var loopRatio = 0.1f;
            // ~1% dense nodes
            final var denseNodeRatio = 0.01f;
            // dense node threshold ~50; pick 64 to be safe
            final var denseNodeNumberOfRels = 64;

            try (var tx = beginTx()) {
                final var write = tx.dataWrite();
                // create finite nodes to create graph from
                // to ensure a connected network of relationships
                final var nodeIds =
                        Stream.generate(write::nodeCreate).limit(numberOfNodes).collect(Collectors.toList());

                // when   relationships are created
                // when   and tracked against a query
                for (long counter = 0; counter < numberOfRelationships; ) {
                    counter += (numberOfRelationships - counter) >= denseNodeNumberOfRels
                                    && random.nextFloat() < denseNodeRatio
                            ? createDense(
                                    write,
                                    indexName,
                                    loopRatio,
                                    denseNodeNumberOfRels,
                                    counter,
                                    nodeIds,
                                    relTypeRanges,
                                    relsWithRelTypeId)
                            : createSingle(
                                    write, indexName, loopRatio, counter, nodeIds, relTypeRanges, relsWithRelTypeId);
                }

                tx.commit();
            } catch (Exception e) {
                throw new AssertionError("failed to create database", e);
            }

            try (var tx = beginTx()) {
                // then   and the number created should be less than or equal to what was asked
                assertThat(tx.dataRead().relationshipsGetCount())
                        .as("relationships created")
                        .isLessThanOrEqualTo(numberOfRelationships);
            } catch (Exception e) {
                throw new AssertionError("failed to count number of relationships", e);
            }

            return new Queries<>(relsWithRelTypeId);
        }

        private int createDense(
                Write write,
                String indexName,
                float loopRatio,
                int denseNodeNumberOfRels,
                long counter,
                List<Long> nodeIds,
                Map<Integer, List<Range>> relTypeRanges,
                EntityIdsMatchingQuery<TokenScanQuery> relsWithRelTypeId)
                throws EntityNotFoundException {
            final var sourceNodeId = random.among(nodeIds);
            nodeIds.remove(sourceNodeId);
            for (long maxPotentialRels = counter + denseNodeNumberOfRels; counter < maxPotentialRels; ) {
                counter += createSingle(
                        write, indexName, loopRatio, counter, sourceNodeId, nodeIds, relTypeRanges, relsWithRelTypeId);
            }
            return denseNodeNumberOfRels;
        }

        private int createSingle(
                Write write,
                String indexName,
                float loopRatio,
                long counter,
                List<Long> nodeIds,
                Map<Integer, List<Range>> relTypeRanges,
                EntityIdsMatchingQuery<TokenScanQuery> relsWithRelTypeId)
                throws EntityNotFoundException {
            final var sourceNodeId = random.among(nodeIds);
            return createSingle(
                    write, indexName, loopRatio, counter, sourceNodeId, nodeIds, relTypeRanges, relsWithRelTypeId);
        }

        private int createSingle(
                Write write,
                String indexName,
                float loopRatio,
                long counter,
                long sourceNodeId,
                List<Long> nodeIds,
                Map<Integer, List<Range>> relTypeRanges,
                EntityIdsMatchingQuery<TokenScanQuery> relsWithRelTypeId)
                throws EntityNotFoundException {
            // randomly select a relType that is within range, or default if none
            final var potentialRelTypeIds = relTypeRanges.entrySet().stream()
                    .filter(entry -> entry.getValue().stream().anyMatch(range -> range.contains(counter)))
                    .mapToInt(Map.Entry::getKey)
                    .toArray();
            final var usingDefault = potentialRelTypeIds.length == 0;
            final var relTypeId = !usingDefault ? random.among(potentialRelTypeIds) : defaultRelType;
            final var targetNodeId = random.nextFloat() < loopRatio ? sourceNodeId : random.among(nodeIds);
            final var relId = write.relationshipCreate(sourceNodeId, relTypeId, targetNodeId);
            if (!usingDefault) {
                relsWithRelTypeId
                        .getOrCreate(new TokenScanQuery(indexName, new TokenPredicate(relTypeId)))
                        .add(relId);
            }
            return 1;
        }
    }
}
