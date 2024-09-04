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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.Query;
import org.neo4j.test.RandomSupport;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class PropertyIndexPartitionedScanTestSuite<QUERY extends Query<?>, CURSOR extends Cursor>
        implements PartitionedScanTestSuite.TestSuite<QUERY, IndexReadSession, CURSOR> {
    private final TestIndexType index;

    PropertyIndexPartitionedScanTestSuite(TestIndexType index) {
        this.index = index;
    }

    protected final Iterable<IndexPrototype> createIndexPrototypes(int tokenId, int[] propKeyIds) {
        final var factory = (PartitionedScanFactories.PropertyIndex<QUERY, CURSOR>) getFactory();

        // IndexPrototype doesn't override hashCode or equal, and we do not wish to alter production code for test;
        // therefore using Pair as a wrapper for what is important
        return Stream.concat(
                        Arrays.stream(propKeyIds)
                                .mapToObj(propKeyId -> Pair.of(
                                        factory.getSchemaDescriptor(tokenId, propKeyId),
                                        factory.getIndexName(tokenId, propKeyId))),
                        Stream.of(Pair.of(
                                factory.getSchemaDescriptor(tokenId, propKeyIds),
                                factory.getIndexName(tokenId, propKeyIds))))
                .distinct()
                .map(indexFrom -> IndexPrototype.forSchema(indexFrom.first())
                        .withIndexType(index.type())
                        .withIndexProvider(index.descriptor())
                        .withName(indexFrom.other()))
                .toList();
    }

    abstract static class WithoutData<QUERY extends Query<?>, CURSOR extends Cursor>
            extends PartitionedScanTestSuite.WithoutData<QUERY, IndexReadSession, CURSOR> {
        protected final PartitionedScanFactories.PropertyIndex<QUERY, CURSOR> factory;

        WithoutData(PropertyIndexPartitionedScanTestSuite<QUERY, CURSOR> testSuite) {
            super(testSuite);
            factory = (PartitionedScanFactories.PropertyIndex<QUERY, CURSOR>) testSuite.getFactory();
        }
    }

    abstract static class WithData<QUERY extends Query<?>, CURSOR extends Cursor>
            extends PartitionedScanTestSuite.WithData<QUERY, IndexReadSession, CURSOR> {
        protected final PartitionedScanFactories.PropertyIndex<QUERY, CURSOR> factory;

        protected abstract Queries<QUERY> createData(int numberOfProperties, int tokenId, int[] propKeyIds);

        WithData(PropertyIndexPartitionedScanTestSuite<QUERY, CURSOR> testSuite) {
            super(testSuite);
            this.factory = (PartitionedScanFactories.PropertyIndex<QUERY, CURSOR>) testSuite.getFactory();
        }
    }

    protected record PropertyRecord(int id, Value value, ValueType type) {}

    protected static PropertyRecord createRandomPropertyRecord(RandomSupport random, int propKeyId, int value) {
        final var type = random.among(ValueType.values());
        return new PropertyRecord(propKeyId, type.toValue(value), type);
    }

    protected enum ValueType {
        NUMBER {
            @Override
            protected Integer createUnderlyingValue(int value) {
                return value;
            }
        },

        NUMBER_ARRAY {
            @Override
            protected Integer[] createUnderlyingValue(int value) {
                return splitNumber(value)
                        .mapToObj(NUMBER::createUnderlyingValue)
                        .map(Integer.class::cast)
                        .toArray(Integer[]::new);
            }
        },

        TEXT {
            @Override
            protected String createUnderlyingValue(int value) {
                return String.valueOf(value);
            }
        },

        TEXT_ARRAY {
            @Override
            protected String[] createUnderlyingValue(int value) {
                return splitNumber(value)
                        .mapToObj(TEXT::createUnderlyingValue)
                        .map(String.class::cast)
                        .toArray(String[]::new);
            }
        },

        GEOMETRY {
            @Override
            protected PointValue createUnderlyingValue(int value) {
                return Values.pointValue(
                        CoordinateReferenceSystem.CARTESIAN,
                        splitNumber(value).asDoubleStream().toArray());
            }
        },

        GEOMETRY_ARRAY {
            @Override
            protected PointValue[] createUnderlyingValue(int value) {
                return splitNumber(value)
                        .mapToObj(GEOMETRY::createUnderlyingValue)
                        .map(PointValue.class::cast)
                        .toArray(PointValue[]::new);
            }
        },

        TEMPORAL {
            @Override
            protected ZonedDateTime createUnderlyingValue(int value) {
                return ZonedDateTime.ofInstant(Instant.ofEpochSecond(value), ZoneOffset.UTC);
            }
        },

        TEMPORAL_ARRAY {
            @Override
            protected ZonedDateTime[] createUnderlyingValue(int value) {
                return splitNumber(value)
                        .mapToObj(TEMPORAL::createUnderlyingValue)
                        .map(ZonedDateTime.class::cast)
                        .toArray(ZonedDateTime[]::new);
            }
        },

        BOOLEAN {
            @Override
            protected Boolean createUnderlyingValue(int value) {
                return value % 2 == 0;
            }
        },

        BOOLEAN_ARRAY {
            @Override
            protected Boolean[] createUnderlyingValue(int value) {
                return splitNumber(value)
                        .mapToObj(BOOLEAN::createUnderlyingValue)
                        .map(Boolean.class::cast)
                        .toArray(Boolean[]::new);
            }
        };

        protected abstract Object createUnderlyingValue(int value);

        protected IntStream splitNumber(int value) {
            final int mask = Short.MAX_VALUE;
            final int x = value & mask;
            final int y = (value & ~mask) >> Short.SIZE;
            return IntStream.of(x, y);
        }

        public Value toValue(int value) {
            return Values.of(createUnderlyingValue(value));
        }
    }

    protected enum TestIndexType {
        RANGE(IndexType.RANGE, AllIndexProviderDescriptors.RANGE_DESCRIPTOR);

        private final IndexType type;
        private final IndexProviderDescriptor descriptor;

        TestIndexType(IndexType type, IndexProviderDescriptor descriptor) {
            this.type = type;
            this.descriptor = descriptor;
        }

        final IndexType type() {
            return type;
        }

        final IndexProviderDescriptor descriptor() {
            return descriptor;
        }
    }
}
