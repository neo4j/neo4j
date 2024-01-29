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

import java.util.Arrays;
import java.util.Objects;
import org.junit.jupiter.api.Nested;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.NodePropertyIndexScan;

abstract class NodePropertyIndexScanPartitionedScanTestSuite
        extends PropertyIndexScanPartitionedScanTestSuite<NodeValueIndexCursor> {
    NodePropertyIndexScanPartitionedScanTestSuite(TestIndexType index) {
        super(index);
    }

    @Override
    public final NodePropertyIndexScan getFactory() {
        return NodePropertyIndexScan.FACTORY;
    }

    @Nested
    class WithoutData extends PropertyIndexScanPartitionedScanTestSuite.WithoutData<NodeValueIndexCursor> {
        WithoutData() {
            super(NodePropertyIndexScanPartitionedScanTestSuite.this);
        }

        @Override
        Queries<PropertyKeyScanQuery> setupDatabase() {
            final var numberOfLabels = 1;
            final var numberOfPropKeys = 2;

            final var labelId = createTokens(numberOfLabels, factory.getTokenSupplier())[0];
            final var propKeyIds = createTokens(numberOfPropKeys, factory.getPropKeySupplier());

            final var data = emptyQueries(labelId, propKeyIds);
            createIndexes(createIndexPrototypes(labelId, propKeyIds));
            return data;
        }
    }

    @Nested
    class WithData extends PropertyIndexScanPartitionedScanTestSuite.WithData<NodeValueIndexCursor> {
        WithData() {
            super(NodePropertyIndexScanPartitionedScanTestSuite.this);
        }

        @Override
        Queries<PropertyKeyScanQuery> setupDatabase() {
            final var numberOfLabels = 1;
            final var numberOfPropKeys = 2;
            final var numberOfProperties = 1 << 12;

            final var labelId = createTokens(numberOfLabels, factory.getTokenSupplier())[0];
            final var propKeyIds = createTokens(numberOfPropKeys, factory.getPropKeySupplier());

            final var data = createData(numberOfProperties, labelId, propKeyIds);
            createIndexes(createIndexPrototypes(labelId, propKeyIds));
            return data;
        }

        @Override
        protected Queries<PropertyKeyScanQuery> createData(int numberOfProperties, int labelId, int[] propKeyIds) {
            // given  a set of queries
            final var nodesInIndex = new EntityIdsMatchingQuery<PropertyKeyScanQuery>();

            // given  a number of properties to create
            final var propValues = random.ints(numberOfProperties).iterator();
            var numberOfCreatedProperties = 0;
            try (var tx = beginTx()) {
                final var write = tx.dataWrite();
                while (propValues.hasNext()) {
                    final var assignedProperties = new PropertyRecord[propKeyIds.length];
                    final var nodeId = write.nodeCreate();
                    if (write.nodeAddLabel(nodeId, labelId)) {
                        for (int i = 0; i < propKeyIds.length; i++) {
                            if (propValues.hasNext()) {
                                // when   properties are created
                                final var prop = createRandomPropertyRecord(random, propKeyIds[i], propValues.next());
                                write.nodeSetProperty(nodeId, prop.id(), prop.value());
                                numberOfCreatedProperties++;
                                assignedProperties[i] = prop;
                                // when   and tracked against queries
                                nodesInIndex
                                        .getOrCreate(new PropertyKeyScanQuery(factory.getIndexName(labelId, prop.id())))
                                        .add(nodeId);
                            }
                        }
                        if (Arrays.stream(assignedProperties).allMatch(Objects::nonNull)) {
                            nodesInIndex
                                    .getOrCreate(new PropertyKeyScanQuery(factory.getIndexName(labelId, propKeyIds)))
                                    .add(nodeId);
                        }
                    }
                }

                tx.commit();
            } catch (Exception e) {
                throw new AssertionError("failed to create database", e);
            }

            // then   the number created should be equal to what was asked
            assertThat(numberOfCreatedProperties).as("node properties created").isEqualTo(numberOfProperties);

            return new Queries<>(nodesInIndex);
        }
    }
}
