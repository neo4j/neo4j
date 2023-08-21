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
package org.neo4j.kernel.api.index;

import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

abstract class PropertyIndexProviderCompatibilityTestSuite extends IndexProviderCompatibilityTestSuite {
    @Override
    IndexPrototype indexPrototype() {
        return IndexPrototype.forSchema(forLabel(1000, 100)).withIndexType(indexType());
    }

    IndexPrototype uniqueIndexPrototype() {
        return IndexPrototype.uniqueForSchema(forLabel(1000, 100)).withIndexType(indexType());
    }

    abstract boolean supportsSpatial();

    /**
     * Granular composite queries means queries against composite index that is made up of a mix of exact, range and exists queries.
     * For example: exact match on first column and range scan on seconds column.
     * See {@link org.neo4j.kernel.impl.index.schema.RangeIndexProvider} for further details on supported combinations.
     * @return true if index provider have support granular composite queries.
     */
    boolean supportsGranularCompositeQueries() {
        return false;
    }

    boolean supportsBooleanRangeQueries() {
        return false;
    }

    boolean supportFullValuePrecisionForNumbers() {
        return true;
    }

    boolean supportsContainsAndEndsWithQueries() {
        return true;
    }

    /**
     * Not all indexes that support spatial types supports all types of queries on them.
     */
    boolean supportsBoundingBoxQueries() {
        return supportsSpatial();
    }

    ValueType[] supportedValueTypes() {
        if (!supportsSpatial()) {
            return RandomValues.excluding(
                    ValueType.CARTESIAN_POINT,
                    ValueType.CARTESIAN_POINT_ARRAY,
                    ValueType.CARTESIAN_POINT_3D,
                    ValueType.CARTESIAN_POINT_3D_ARRAY,
                    ValueType.GEOGRAPHIC_POINT,
                    ValueType.GEOGRAPHIC_POINT_ARRAY,
                    ValueType.GEOGRAPHIC_POINT_3D,
                    ValueType.GEOGRAPHIC_POINT_3D_ARRAY);
        }
        return ValueType.values();
    }

    @Nested
    class GeneralSimpleIndexPopulator extends SimpleIndexPopulatorCompatibility.General {
        GeneralSimpleIndexPopulator() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class UniqueSimpleIndexPopulator extends SimpleIndexPopulatorCompatibility.Unique {
        UniqueSimpleIndexPopulator() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class GeneralCompositeIndexPopulator extends CompositeIndexPopulatorCompatibility.General {
        GeneralCompositeIndexPopulator() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class UniqueCompositeIndexPopulator extends CompositeIndexPopulatorCompatibility.Unique {
        UniqueCompositeIndexPopulator() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class GeneralSimpleIndexAccessor extends SimpleIndexAccessorCompatibility.General {
        GeneralSimpleIndexAccessor() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class UniqueSimpleIndexAccessor extends SimpleIndexAccessorCompatibility.Unique {
        UniqueSimpleIndexAccessor() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class GeneralCompositeIndexAccessor extends CompositeIndexAccessorCompatibility.General {
        GeneralCompositeIndexAccessor() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class UniqueCompositeIndexAccessor extends CompositeIndexAccessorCompatibility.Unique {
        UniqueCompositeIndexAccessor() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class SimpleRandomizedIndexAccessor extends SimpleRandomizedIndexAccessorCompatibility {
        SimpleRandomizedIndexAccessor() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class ExactCompositeRandomizedIndexAccessor extends CompositeRandomizedIndexAccessorCompatibility.Exact {
        ExactCompositeRandomizedIndexAccessor() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class RangeCompositeRandomizedIndexAccessor extends CompositeRandomizedIndexAccessorCompatibility.Range {
        RangeCompositeRandomizedIndexAccessor() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class UniqueConstraint extends UniqueConstraintCompatibility {
        UniqueConstraint() {
            super(PropertyIndexProviderCompatibilityTestSuite.this);
        }
    }

    abstract static class Compatibility extends IndexProviderCompatabilityTestBase {
        final PropertyIndexProviderCompatibilityTestSuite testSuite;
        final List<NodeAndValue> valueSet1;
        final List<NodeAndValue> valueSet2;

        Compatibility(PropertyIndexProviderCompatibilityTestSuite testSuite, IndexPrototype prototype) {
            super(testSuite, prototype);
            this.testSuite = testSuite;
            this.valueSet1 = allValues(
                    testSuite.supportsSpatial(),
                    Arrays.asList(
                            Values.of("string1"),
                            Values.of(42),
                            Values.of(true),
                            Values.of(new char[] {'a', 'z'}),
                            Values.of(new String[] {"arrayString1", "arraysString2"}),
                            Values.of(new byte[] {(byte) 1, (byte) 12}),
                            Values.of(new short[] {314, 1337}),
                            Values.of(new int[] {3140, 13370}),
                            Values.of(new long[] {31400, 133700}),
                            Values.of(new boolean[] {true, true})),
                    Arrays.asList(
                            DateValue.epochDate(2),
                            LocalTimeValue.localTime(100000),
                            TimeValue.time(43_200_000_000_000L, ZoneOffset.UTC), // Noon
                            TimeValue.time(43_201_000_000_000L, ZoneOffset.UTC),
                            TimeValue.time(43_200_000_000_000L, ZoneOffset.of("+01:00")), // Noon in the next time-zone
                            TimeValue.time(46_800_000_000_000L, ZoneOffset.UTC), // Same time UTC as prev time
                            LocalDateTimeValue.localDateTime(2018, 3, 1, 13, 50, 42, 1337),
                            DateTimeValue.datetime(2014, 3, 25, 12, 45, 13, 7474, "UTC"),
                            DateTimeValue.datetime(2014, 3, 25, 12, 45, 13, 7474, "Europe/Stockholm"),
                            DateTimeValue.datetime(2014, 3, 25, 12, 45, 13, 7474, "+05:00"),
                            DateTimeValue.datetime(2015, 3, 25, 12, 45, 13, 7474, "+05:00"),
                            DateTimeValue.datetime(2014, 4, 25, 12, 45, 13, 7474, "+05:00"),
                            DateTimeValue.datetime(2014, 3, 26, 12, 45, 13, 7474, "+05:00"),
                            DateTimeValue.datetime(2014, 3, 25, 13, 45, 13, 7474, "+05:00"),
                            DateTimeValue.datetime(2014, 3, 25, 12, 46, 13, 7474, "+05:00"),
                            DateTimeValue.datetime(2014, 3, 25, 12, 45, 14, 7474, "+05:00"),
                            DateTimeValue.datetime(2014, 3, 25, 12, 45, 13, 7475, "+05:00"),
                            // only runnable if JVM supports East-Saskatchewan
                            // DateTimeValue.datetime( 2001, 1, 25, 11, 11, 30, 0, "Canada/East-Saskatchewan" ),
                            DateTimeValue.datetime(2038, 1, 18, 9, 14, 7, 0, "-18:00"),
                            DateTimeValue.datetime(10000, 100, ZoneOffset.ofTotalSeconds(3)),
                            DateTimeValue.datetime(10000, 101, ZoneOffset.ofTotalSeconds(-3)),
                            DurationValue.duration(10, 20, 30, 40),
                            DurationValue.duration(11, 20, 30, 40),
                            DurationValue.duration(10, 21, 30, 40),
                            DurationValue.duration(10, 20, 31, 40),
                            DurationValue.duration(10, 20, 30, 41),
                            Values.dateTimeArray(new ZonedDateTime[] {
                                ZonedDateTime.of(2018, 10, 9, 8, 7, 6, 5, ZoneId.of("UTC")),
                                ZonedDateTime.of(2017, 9, 8, 7, 6, 5, 4, ZoneId.of("UTC"))
                            }),
                            Values.localDateTimeArray(new LocalDateTime[] {
                                LocalDateTime.of(2018, 10, 9, 8, 7, 6, 5), LocalDateTime.of(2018, 10, 9, 8, 7, 6, 5)
                            }),
                            Values.timeArray(new OffsetTime[] {
                                OffsetTime.of(20, 8, 7, 6, ZoneOffset.UTC), OffsetTime.of(20, 8, 7, 6, ZoneOffset.UTC)
                            }),
                            Values.dateArray(new LocalDate[] {LocalDate.of(1, 12, 28), LocalDate.of(1, 12, 28)}),
                            Values.localTimeArray(new LocalTime[] {LocalTime.of(9, 28), LocalTime.of(9, 28)}),
                            Values.durationArray(new DurationValue[] {
                                DurationValue.duration(12, 10, 10, 10), DurationValue.duration(12, 10, 10, 10)
                            })),
                    Arrays.asList(
                            Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 0, 0),
                            Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.78, 56.7)));

            this.valueSet2 = allValues(
                    testSuite.supportsSpatial(),
                    Arrays.asList(
                            Values.of("string2"),
                            Values.of(1337),
                            Values.of(false),
                            Values.of(new char[] {'b', 'c'}),
                            Values.of(new String[] {"someString1", "someString2"}),
                            Values.of(new byte[] {(byte) 9, (byte) 9}),
                            Values.of(new short[] {99, 999}),
                            Values.of(new int[] {99999, 99999}),
                            Values.of(new long[] {999999, 999999}),
                            Values.of(new boolean[] {false, false})),
                    Arrays.asList(
                            DateValue.epochDate(42),
                            LocalTimeValue.localTime(2000),
                            TimeValue.time(100L, ZoneOffset.UTC), // Just around midnight
                            LocalDateTimeValue.localDateTime(2018, 2, 28, 11, 5, 1, 42),
                            DateTimeValue.datetime(1999, 12, 31, 23, 59, 59, 123456789, "Europe/London"),
                            DurationValue.duration(4, 3, 2, 1),
                            Values.dateTimeArray(new ZonedDateTime[] {
                                ZonedDateTime.of(999, 10, 9, 8, 7, 6, 5, ZoneId.of("UTC")),
                                ZonedDateTime.of(999, 9, 8, 7, 6, 5, 4, ZoneId.of("UTC"))
                            }),
                            Values.localDateTimeArray(new LocalDateTime[] {
                                LocalDateTime.of(999, 10, 9, 8, 7, 6, 5), LocalDateTime.of(999, 10, 9, 8, 7, 6, 5)
                            }),
                            Values.timeArray(new OffsetTime[] {
                                OffsetTime.of(19, 8, 7, 6, ZoneOffset.UTC), OffsetTime.of(19, 8, 7, 6, ZoneOffset.UTC)
                            }),
                            Values.dateArray(new LocalDate[] {LocalDate.of(999, 12, 28), LocalDate.of(999, 12, 28)}),
                            Values.localTimeArray(new LocalTime[] {LocalTime.of(19, 28), LocalTime.of(19, 28)}),
                            Values.durationArray(new DurationValue[] {
                                DurationValue.duration(99, 10, 10, 10), DurationValue.duration(99, 10, 10, 10)
                            })),
                    Arrays.asList(
                            Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 90, 90),
                            Values.pointValue(CoordinateReferenceSystem.WGS_84, 9.21, 9.65)));
        }

        List<ValueIndexEntryUpdate<?>> updates(List<NodeAndValue> values) {
            return updates(values, 0);
        }

        List<ValueIndexEntryUpdate<?>> updates(List<NodeAndValue> values, long nodeIdOffset) {
            List<ValueIndexEntryUpdate<?>> updates = new ArrayList<>();
            values.forEach(entry ->
                    updates.add(ValueIndexEntryUpdate.add(nodeIdOffset + entry.nodeId, descriptor, entry.value)));
            return updates;
        }

        private static List<NodeAndValue> allValues(
                boolean supportsSpatial, List<Value> common, List<Value> temporal, List<Value> spatial) {
            long nodeIds = 0;
            List<NodeAndValue> result = new ArrayList<>();
            for (Value value : common) {
                result.add(new NodeAndValue(nodeIds++, value));
            }
            if (supportsSpatial) {
                for (Value value : spatial) {
                    result.add(new NodeAndValue(nodeIds++, value));
                }
            }
            for (Value value : temporal) {
                result.add(new NodeAndValue(nodeIds++, value));
            }
            return result;
        }

        static class NodeAndValue {
            final long nodeId;
            final Value value;

            NodeAndValue(long nodeId, Value value) {
                this.nodeId = nodeId;
                this.value = value;
            }
        }
    }
}
