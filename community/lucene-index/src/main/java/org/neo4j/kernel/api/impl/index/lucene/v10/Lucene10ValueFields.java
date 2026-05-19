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
package org.neo4j.kernel.api.impl.index.lucene.v10;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Collection;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.neo4j.exceptions.InternalException;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Values;

final class Lucene10ValueFields {
    private Lucene10ValueFields() {}

    static final class BooleanField extends Field {
        private static final FieldType TYPE;

        private static final BytesRef TRUE = new BytesRef(new byte[1]);
        private static final BytesRef FALSE = new BytesRef(new byte[0]);

        static {
            FieldType type = new FieldType();
            type.setTokenized(false);
            type.setIndexOptions(IndexOptions.DOCS);
            type.freeze();
            TYPE = type;
        }

        BooleanField(String name, boolean value) {
            super(name, TYPE);
            this.fieldsData = value ? TRUE : FALSE;
        }

        public boolean getBoolValue() {
            return fieldsData == TRUE;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + " <" + name + ':' + getBoolValue() + '>';
        }

        static TermQuery newExactQuery(String field, boolean value) {
            Term term = new Term(field, value ? TRUE : FALSE);
            return new TermQuery(term);
        }

        static Query newRangeQuery(String field, boolean lowerValueInclusive, boolean upperValueInclusive) {
            if (lowerValueInclusive && !upperValueInclusive) {
                return MatchNoDocsQuery.INSTANCE;
            } else if (lowerValueInclusive) {
                // range from [true, true]
                return newExactQuery(field, true);
            } else if (!upperValueInclusive) {
                // range from [false, false]
                return newExactQuery(field, false);
            } else {
                // range from [false, true]
                return new FieldExistsQuery(field);
            }
        }
    }

    static final class SingleLongField extends Field {
        private static final FieldType TYPE;

        static {
            FieldType type = new FieldType();
            type.setDimensions(1, Long.BYTES);
            type.freeze();
            TYPE = type;
        }

        SingleLongField(String name, long value) {
            super(name, TYPE);
            this.fieldsData = value;
        }

        @Override
        public BytesRef binaryValue() {
            return new BytesRef(longToBytes((Long) fieldsData));
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + " <" + name + ':' + fieldsData + '>';
        }

        static Query newExactQuery(String field, long value) {
            Preconditions.requireNonNull(field, "Field cannot be null");
            byte[] encodedValue = longToBytes(value);
            return new LongPointRangeQuery(field, encodedValue, encodedValue, 1);
        }

        static Query newSetQuery(String field, Collection<Long> values) {
            Preconditions.requireNonNull(field, "Field cannot be null");
            return LongPoint.newSetQuery(field, values);
        }

        static Query newRangeQuery(String field, long lowerValueInclusive, long upperValueInclusive) {
            Preconditions.requireNonNull(field, "Field cannot be null");
            if (upperValueInclusive < lowerValueInclusive) {
                return MatchNoDocsQuery.INSTANCE;
            }
            return new LongPointRangeQuery(
                    field, longToBytes(lowerValueInclusive), longToBytes(upperValueInclusive), 1);
        }

        private static final class LongPointRangeQuery extends PointRangeQuery {

            private LongPointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims) {
                super(field, lowerPoint, upperPoint, numDims);
            }

            @Override
            protected String toString(int dimension, byte[] value) {
                return Long.toString(NumericUtils.sortableBytesToLong(value, 0));
            }
        }
    }

    static final class SingleIntegerField extends Field {
        private static final FieldType TYPE;

        static {
            FieldType type = new FieldType();
            type.setDimensions(1, Integer.BYTES);
            type.freeze();
            TYPE = type;
        }

        SingleIntegerField(String name, int value) {
            super(name, TYPE);
            fieldsData = value;
        }

        @Override
        public BytesRef binaryValue() {
            return new BytesRef(intToBytes((Integer) fieldsData));
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + " <" + name + ':' + fieldsData + '>';
        }

        static Query newExactQuery(String field, int value) {
            Preconditions.requireNonNull(field, "Field cannot be null");
            byte[] encodedValue = intToBytes(value);
            return new IntegerPointRangeQuery(field, encodedValue, encodedValue, 1);
        }

        static Query newRangeQuery(String field, int lowerValueInclusive, int upperValueInclusive) {
            Preconditions.requireNonNull(field, "Field cannot be null");
            if (upperValueInclusive < lowerValueInclusive) {
                return MatchNoDocsQuery.INSTANCE;
            }
            return new IntegerPointRangeQuery(
                    field, intToBytes(lowerValueInclusive), intToBytes(upperValueInclusive), 1);
        }

        private static final class IntegerPointRangeQuery extends PointRangeQuery {

            private IntegerPointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims) {
                super(field, lowerPoint, upperPoint, numDims);
            }

            @Override
            protected String toString(int dimension, byte[] value) {
                return Integer.toString(NumericUtils.sortableBytesToInt(value, 0));
            }
        }
    }

    static final class SingleInstantField extends Field {
        private static final FieldType TYPE;

        static {
            FieldType type = new FieldType();
            type.setDimensions(1, INSTANT_BYTES);
            type.freeze();
            TYPE = type;
        }

        SingleInstantField(String name, Instant value) {
            super(name, TYPE);
            fieldsData = value;
        }

        @Override
        public BytesRef binaryValue() {
            return new BytesRef(instantToBytes((Instant) fieldsData));
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + " <" + name + ':' + fieldsData + '>';
        }

        static Query newExactQuery(String field, Instant value) {
            Preconditions.requireNonNull(field, "Field cannot be null");
            byte[] encodedValue = instantToBytes(value);
            return new InstantPointRangeQuery(field, encodedValue, encodedValue, 1);
        }

        static Query newRangeQuery(String field, Instant lowerValueInclusive, Instant upperValueInclusive) {
            Preconditions.requireNonNull(field, "Field cannot be null");
            if (lowerValueInclusive.isAfter(upperValueInclusive)) {
                return MatchNoDocsQuery.INSTANCE;
            }
            return new InstantPointRangeQuery(
                    field, instantToBytes(lowerValueInclusive), instantToBytes(upperValueInclusive), 1);
        }

        private static final class InstantPointRangeQuery extends PointRangeQuery {

            private InstantPointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims) {
                super(field, lowerPoint, upperPoint, numDims);
            }

            @Override
            protected String toString(int dimension, byte[] value) {
                return bytesToInstant(value).toString();
            }
        }
    }

    static final class SingleDoubleField extends Field {
        private static final FieldType TYPE;

        static {
            FieldType type = new FieldType();
            type.setDimensions(1, Double.BYTES);
            type.freeze();
            TYPE = type;
        }

        SingleDoubleField(String name, double value) {
            super(name, TYPE);
            this.fieldsData = value;
        }

        @Override
        public BytesRef binaryValue() {
            return new BytesRef(doubleToBytes((Double) fieldsData));
        }

        @Override
        public void setDoubleValue(double value) {
            super.setLongValue(NumericUtils.doubleToSortableLong(value));
        }

        @Override
        public void setLongValue(long value) {
            throw new IllegalArgumentException("Cannot change value type from Double to Long");
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + " <" + name + ':' + fieldsData + '>';
        }

        static Query newExactQuery(String field, double value) {
            Preconditions.requireNonNull(field, "Field cannot be null");
            byte[] encodedValue = doubleToBytes(value);
            return new DoublePointRangeQuery(field, encodedValue, encodedValue, 1);
        }

        static Query newSetQuery(String field, Collection<Double> values) {
            Preconditions.requireNonNull(field, "Field cannot be null");
            return DoublePoint.newSetQuery(field, values);
        }

        static Query newRangeQuery(String field, double lowerValueInclusive, double upperValueInclusive) {
            Preconditions.requireNonNull(field, "field cannot be null");
            Preconditions.checkArgument(
                    !Double.isNaN(lowerValueInclusive), "NaN is not a valid lower bound for a range");
            Preconditions.checkArgument(
                    !Double.isNaN(upperValueInclusive), "NaN is not a valid upper bound for a range");
            if (lowerValueInclusive > upperValueInclusive) {
                return MatchNoDocsQuery.INSTANCE;
            }
            return new DoublePointRangeQuery(
                    field, doubleToBytes(lowerValueInclusive), doubleToBytes(upperValueInclusive), 1);
        }

        private static byte[] doubleToBytes(double value) {
            return longToBytes(NumericUtils.doubleToSortableLong(value));
        }

        private static final class DoublePointRangeQuery extends PointRangeQuery {

            private DoublePointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims) {
                super(field, lowerPoint, upperPoint, numDims);
            }

            @Override
            protected String toString(int dimension, byte[] value) {
                return Double.toString(NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(value, 0)));
            }
        }
    }

    public static <T extends Temporal, V extends TemporalValue<T, V>> TemporalWithZone<T, V> storedFromTemporal(
            TemporalValue<T, V> tv) {
        return new TemporalWithZone<>(tv);
    }

    static byte[] intToBytes(int value) {
        byte[] data = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(value, data, 0);
        return data;
    }

    static byte[] longToBytes(long value) {
        byte[] data = new byte[Long.BYTES];
        NumericUtils.longToSortableBytes(value, data, 0);
        return data;
    }

    private static final int INSTANT_BYTES = Long.BYTES + Integer.BYTES;

    private static byte[] instantToBytes(Instant value) {
        byte[] data = new byte[INSTANT_BYTES];
        NumericUtils.longToSortableBytes(value.getEpochSecond(), data, 0);
        NumericUtils.intToSortableBytes(value.getNano(), data, Long.BYTES);
        return data;
    }

    private static Instant bytesToInstant(byte[] data) {
        long seconds = NumericUtils.sortableBytesToLong(data, 0);
        int nanos = NumericUtils.sortableBytesToInt(data, Long.BYTES);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    record TemporalOffsetWithId(ZoneOffset zoneOffset, ZoneId zoneId) {

        boolean hasZoneOffset() {
            return zoneOffset != null;
        }

        boolean hasZoneId() {
            return zoneId != null;
        }

        static ZoneOffset zoneOffsetOf(TemporalOffsetWithId temporalOffsetWithId, ZoneOffset orElse) {
            if (temporalOffsetWithId != null && temporalOffsetWithId.hasZoneOffset()) {
                return temporalOffsetWithId.zoneOffset;
            } else {
                return orElse;
            }
        }

        static boolean hasZoneId(TemporalOffsetWithId temporalOffsetWithId) {
            return temporalOffsetWithId != null && temporalOffsetWithId.hasZoneId();
        }

        static boolean hasZoneOffset(TemporalOffsetWithId temporalOffsetWithId) {
            return temporalOffsetWithId != null && temporalOffsetWithId.hasZoneOffset();
        }
    }

    /// All the fields necessary to store a temporal value, with offsets if they exist
    /// zoneOffset can be null. In that case, zoneId is always null.
    /// zoneOffset can exist without zoneId.
    /// This is required to support compatibility with the esoteric Cypher temporal ordering rules.
    /// Order is by instant (seconds, nanos).
    /// The zone offset is a tie-breaker for equal instants. -ve offset is earlier.
    /// The zone id is a tie-breaker for equal zone offsets.
    record TemporalWithZone<T extends Temporal, V extends TemporalValue<T, V>>(TemporalValue<T, V> value)
            implements Comparable<TemporalWithZone<T, V>> {

        TemporalWithZone(TemporalValue<T, V> value) {
            this.value = value;
            Preconditions.requireNonNull(value, "value must not be null");
        }

        /// Instant values returned are only comparable for the same subtypes of temporal values
        /// This is fine because different types are stored in different namespaces,
        /// so they are never actually compared.
        Instant instant() {
            return switch (value) {
                case DateTimeValue dateTimeValue -> dateTimeValue.asObjectCopy().toInstant();

                case LocalDateTimeValue localDateTimeValue -> {
                    LocalDateTime dateTime = localDateTimeValue.asObjectCopy();
                    yield Instant.ofEpochSecond(dateTime.toEpochSecond(ZoneOffset.UTC), dateTime.getNano());
                }

                case LocalTimeValue localTimeValue -> {
                    Duration offset = Duration.between(LocalTime.MIN, localTimeValue.asObjectCopy());
                    yield Instant.ofEpochSecond(offset.getSeconds(), offset.getNano());
                }

                case DateValue dateValue ->
                // 00:00:00 UTC on the date in question
                {
                    LocalDate localDate = dateValue.asObjectCopy();
                    yield Instant.ofEpochSecond(localDate.toEpochSecond(LocalTime.ofSecondOfDay(0), ZoneOffset.UTC), 0);
                }

                case TimeValue timeValue -> {
                    // a fake instant calculated as duration from the earliest possible offset time
                    Duration offset = Duration.between(OffsetTime.MIN, timeValue.asObjectCopy());
                    yield Instant.ofEpochSecond(offset.getSeconds(), offset.getNano());
                }
                // would be nice to make Temporal sealed, and remove this default branch
                default ->
                    throw InternalException.internalError(
                            this.getClass().getSimpleName(), "Temporal type is not supported");
            };
        }

        ZoneOffset zoneOffset() {
            return switch (value) {
                case DateTimeValue dateTimeValue -> dateTimeValue.asObjectCopy().getOffset();
                case TimeValue timeValue -> timeValue.asObjectCopy().getOffset();
                default ->
                    throw InternalException.internalError(
                            this.getClass().getSimpleName(), "Temporal type is not supported");
            };
        }

        boolean hasZoneOffset() {
            return switch (value) {
                case DateTimeValue ignored -> true;
                case TimeValue ignored -> true;
                default -> false;
            };
        }

        ZoneId zoneId() {
            if (value instanceof DateTimeValue dateTimeValue) {
                return dateTimeValue.asObjectCopy().getZone();
            }
            throw InternalException.internalError(this.getClass().getSimpleName(), "Temporal type is not supported");
        }

        boolean hasZoneId() {
            return switch (value) {
                case DateTimeValue ignored -> true;
                default -> false;
            };
        }

        TemporalOffsetWithId temporalOffsetWithId() {
            ZoneOffset zoneOffset = hasZoneOffset() ? zoneOffset() : null;
            ZoneId zoneId = hasZoneId() ? zoneId() : null;
            return new TemporalOffsetWithId(zoneOffset, zoneId);
        }

        @Override
        public int compareTo(TemporalWithZone<T, V> o) {
            return compare(this, o);
        }

        public static int compare(TemporalWithZone<?, ?> twzFrom, TemporalWithZone<?, ?> twzTo) {
            return Values.COMPARATOR.compare(twzFrom.value, twzTo.value);
        }

        /// Respect the DateTimeValue ordering of zone ids
        /// which is offset always precedes region.
        static String zoneIdString(ZoneId zoneId) {
            if (zoneId instanceof ZoneOffset) {
                return "Offset::" + zoneId.getId();
            } else {
                return "Region::" + TimeZones.map(TimeZones.map(zoneId.getId()));
            }
        }
    }
}
