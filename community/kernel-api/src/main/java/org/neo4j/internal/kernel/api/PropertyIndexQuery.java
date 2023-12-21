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
package org.neo4j.internal.kernel.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNullElse;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.utf8Value;

import java.util.Arrays;
import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.UTF8StringValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

public abstract class PropertyIndexQuery implements IndexQuery {
    /**
     * Scans over the whole index
     *
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static AllEntriesPredicate allEntries() {
        return new AllEntriesPredicate();
    }

    /**
     * Searches the index for all entries that has the given property.
     *
     * @param propertyKeyId the property ID to match.
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static ExistsPredicate exists(int propertyKeyId) {
        return new ExistsPredicate(propertyKeyId);
    }

    /**
     * Searches the index for a certain value.
     *
     * @param propertyKeyId the property ID to match.
     * @param value the property value to search for.
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static ExactPredicate exact(int propertyKeyId, Object value) {
        var exactValue = value instanceof Value ? (Value) value : Values.of(value);
        if (AnyValue.isNaN(exactValue)) {
            return new IncomparableExactPredicate(propertyKeyId, exactValue);
        }
        return new ExactPredicate(propertyKeyId, exactValue);
    }

    /**
     * Searches the index for numeric values between {@code from} and {@code to}.
     *
     * @param propertyKeyId the property ID to match.
     * @param from lower bound of the range or null if unbounded
     * @param fromInclusive the lower bound is inclusive if true.
     * @param to upper bound of the range or null if unbounded
     * @param toInclusive the upper bound is inclusive if true.
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static RangePredicate<?> range(
            int propertyKeyId, Number from, boolean fromInclusive, Number to, boolean toInclusive) {
        return range(
                propertyKeyId,
                from == null ? null : Values.numberValue(from),
                fromInclusive,
                to == null ? null : Values.numberValue(to),
                toInclusive);
    }

    public static RangePredicate<?> range(
            int propertyKeyId, String from, boolean fromInclusive, String to, boolean toInclusive) {
        return new TextRangePredicate(
                propertyKeyId,
                from == null ? null : Values.stringValue(from),
                fromInclusive,
                to == null ? null : Values.stringValue(to),
                toInclusive);
    }

    public static <VALUE extends Value> RangePredicate<?> range(
            int propertyKeyId, VALUE from, boolean fromInclusive, VALUE to, boolean toInclusive) {
        if (from == null && to == null) {
            throw new IllegalArgumentException("Cannot create RangePredicate without at least one bound");
        }

        ValueGroup valueGroup = requireNonNullElse(from, to).valueGroup();
        return switch (valueGroup) {
            case NUMBER -> AnyValue.hasNaNOperand(from, to)
                    // When the range bounds are explicitly set to NaN, we don't want to find anything
                    // because any comparison with NaN is false.
                    ? new IncomparableRangePredicate<>(
                            propertyKeyId, ValueGroup.NUMBER, from, fromInclusive, to, toInclusive)
                    : new NumberRangePredicate(
                            propertyKeyId, (NumberValue) from, fromInclusive, (NumberValue) to, toInclusive);

            case TEXT -> new TextRangePredicate(
                    propertyKeyId, (TextValue) from, fromInclusive, (TextValue) to, toInclusive);

            case DURATION, DURATION_ARRAY, GEOMETRY, GEOMETRY_ARRAY -> {
                if (fromInclusive && to == null) {
                    yield new RangePredicate<>(propertyKeyId, valueGroup, from, true, from, true);
                } else if (toInclusive && from == null) {
                    yield new RangePredicate<>(propertyKeyId, valueGroup, to, true, to, true);
                } else {
                    yield new IncomparableRangePredicate<>(
                            propertyKeyId, valueGroup, from, fromInclusive, to, toInclusive);
                }
            }

            default -> new RangePredicate<>(propertyKeyId, valueGroup, from, fromInclusive, to, toInclusive);
        };
    }

    public static BoundingBoxPredicate boundingBox(int propertyKeyId, PointValue from, PointValue to) {
        return new BoundingBoxPredicate(propertyKeyId, from, to);
    }

    public static BoundingBoxPredicate boundingBox(
            int propertyKeyId, PointValue from, PointValue to, boolean inclusive) {
        return new BoundingBoxPredicate(propertyKeyId, from, to, inclusive);
    }

    /**
     * Searches the index string values starting with {@code prefix}.
     *
     * @param propertyKeyId the property ID to match.
     * @param prefix the string prefix to search for.
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static StringPrefixPredicate stringPrefix(int propertyKeyId, TextValue prefix) {
        return new StringPrefixPredicate(propertyKeyId, prefix);
    }

    /**
     * Searches the index for string values containing the exact search string.
     *
     * @param propertyKeyId the property ID to match.
     * @param contains the string to search for.
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static StringContainsPredicate stringContains(int propertyKeyId, TextValue contains) {
        return new StringContainsPredicate(propertyKeyId, contains);
    }

    /**
     * Searches the index string values ending with {@code suffix}.
     *
     * @param propertyKeyId the property ID to match.
     * @param suffix the string suffix to search for.
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static StringSuffixPredicate stringSuffix(int propertyKeyId, TextValue suffix) {
        return new StringSuffixPredicate(propertyKeyId, suffix);
    }

    public static PropertyIndexQuery fulltextSearch(String query) {
        return new FulltextSearchPredicate(query);
    }

    public static PropertyIndexQuery fulltextSearch(String query, String queryAnalyzer) {
        return new FulltextSearchPredicate(query, queryAnalyzer);
    }

    public static PropertyIndexQuery nearestNeighbors(int k, float[] query) {
        return new NearestNeighborsPredicate(k, query);
    }

    public static ValueTuple asValueTuple(PropertyIndexQuery.ExactPredicate... query) {
        Value[] values = new Value[query.length];
        for (int i = 0; i < query.length; i++) {
            values[i] = query[i].value();
        }
        return ValueTuple.of(values);
    }

    private final int propertyKeyId;

    protected PropertyIndexQuery(int propertyKeyId) {
        this.propertyKeyId = propertyKeyId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PropertyIndexQuery that = (PropertyIndexQuery) o;
        return propertyKeyId == that.propertyKeyId;
    }

    @Override
    public int hashCode() {
        return propertyKeyId + getClass().hashCode();
    }

    @Override
    public final String toString() {
        // Only used to debugging, it's okay to be a bit slow.
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    /**
     * @return The ID of the property key, this queries against.
     */
    public final int propertyKeyId() {
        return propertyKeyId;
    }

    /**
     * @param value to test against the query.
     * @return true if the {@code value} satisfies the query; false otherwise.
     */
    public abstract boolean acceptsValue(Value value);

    public boolean acceptsValueAt(PropertyCursor property) {
        return acceptsValue(property.propertyValue());
    }

    /**
     * @return Target {@link ValueGroup} for query or {@link ValueGroup#UNKNOWN} if not targeting single group.
     */
    public abstract ValueGroup valueGroup();

    /**
     * @return Target {@link ValueCategory} for query
     */
    @Override
    public ValueCategory valueCategory() {
        return valueGroup().category();
    }

    public static final class AllEntriesPredicate extends PropertyIndexQuery {
        private AllEntriesPredicate() {
            super(TokenConstants.ANY_PROPERTY_KEY);
        }

        @Override
        public boolean acceptsValue(Value value) {
            return value != null && value != NO_VALUE;
        }

        @Override
        public ValueGroup valueGroup() {
            return ValueGroup.UNKNOWN;
        }

        @Override
        public IndexQueryType type() {
            return IndexQueryType.ALL_ENTRIES;
        }
    }

    public static final class ExistsPredicate extends PropertyIndexQuery {
        private ExistsPredicate(int propertyKeyId) {
            super(propertyKeyId);
        }

        @Override
        public IndexQueryType type() {
            return IndexQueryType.EXISTS;
        }

        @Override
        public boolean acceptsValue(Value value) {
            return value != null && value != NO_VALUE;
        }

        @Override
        public boolean acceptsValueAt(PropertyCursor property) {
            return true;
        }

        @Override
        public ValueGroup valueGroup() {
            return ValueGroup.UNKNOWN;
        }
    }

    public static class ExactPredicate extends PropertyIndexQuery {
        private final Value exactValue;

        private ExactPredicate(int propertyKeyId, Value value) {
            super(propertyKeyId);
            this.exactValue = value;
        }

        @Override
        public IndexQueryType type() {
            return IndexQueryType.EXACT;
        }

        @Override
        public boolean acceptsValue(Value value) {
            return exactValue.equals(value);
        }

        @Override
        public ValueGroup valueGroup() {
            return exactValue.valueGroup();
        }

        public Value value() {
            return exactValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ExactPredicate that = (ExactPredicate) o;
            return Objects.equals(exactValue, that.exactValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), exactValue);
        }
    }

    public static class RangePredicate<T extends Value> extends PropertyIndexQuery {
        protected final T from;
        protected final boolean fromInclusive;
        protected final T to;
        protected final boolean toInclusive;
        protected final ValueGroup valueGroup;

        private RangePredicate(
                int propertyKeyId, ValueGroup valueGroup, T from, boolean fromInclusive, T to, boolean toInclusive) {
            super(propertyKeyId);
            this.valueGroup = valueGroup;
            this.from = from;
            this.fromInclusive = fromInclusive;
            this.to = to;
            this.toInclusive = toInclusive;
        }

        @Override
        public IndexQueryType type() {
            return IndexQueryType.RANGE;
        }

        @Override
        public boolean acceptsValue(Value value) {
            if (value == null || value == NO_VALUE || value.valueGroup() != valueGroup) {
                return false;
            }
            if (from != null) {
                int compare = Values.COMPARATOR.compare(value, from);
                if (compare < 0 || !fromInclusive && compare == 0) {
                    return false;
                }
            }
            if (to != null) {
                int compare = Values.COMPARATOR.compare(value, to);
                return compare <= 0 && (toInclusive || compare != 0);
            }
            return true;
        }

        @Override
        public ValueGroup valueGroup() {
            return valueGroup;
        }

        public Value fromValue() {
            return from == null ? NO_VALUE : from;
        }

        public Value toValue() {
            return to == null ? NO_VALUE : to;
        }

        public boolean fromInclusive() {
            return fromInclusive;
        }

        public boolean toInclusive() {
            return toInclusive;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            RangePredicate<?> that = (RangePredicate<?>) o;
            return fromInclusive == that.fromInclusive
                    && toInclusive == that.toInclusive
                    && Objects.equals(from, that.from)
                    && Objects.equals(to, that.to)
                    && valueGroup == that.valueGroup;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), from, fromInclusive, to, toInclusive, valueGroup);
        }
    }

    public static final class NumberRangePredicate extends RangePredicate<NumberValue> {
        private NumberRangePredicate(
                int propertyKeyId, NumberValue from, boolean fromInclusive, NumberValue to, boolean toInclusive) {
            // A little something about NaN.
            // For range queries with numbers we need to redefine the upper bound from NaN to positive infinity.
            // The reason is that we do not want to find NaNs for seeks, but for full scans we do.
            // The index will treat open upper bound (null) as scan to highest possible value. According to the index
            // this is NaN, but we don't want to include that so we translate null to Double.POSITIVE_INFINITY here.
            super(
                    propertyKeyId,
                    ValueGroup.NUMBER,
                    from,
                    fromInclusive,
                    requireNonNullElse(to, Values.doubleValue(Double.POSITIVE_INFINITY)),
                    to == null || toInclusive);
        }

        public Number from() {
            return from == null ? null : from.asObject();
        }

        public Number to() {
            return to.asObject();
        }
    }

    public static final class TextRangePredicate extends RangePredicate<TextValue> {
        private TextRangePredicate(
                int propertyKeyId, TextValue from, boolean fromInclusive, TextValue to, boolean toInclusive) {
            super(propertyKeyId, ValueGroup.TEXT, from, fromInclusive, to, toInclusive);
        }

        public String from() {
            return from == null ? null : from.stringValue();
        }

        public String to() {
            return to == null ? null : to.stringValue();
        }
    }

    public static final class BoundingBoxPredicate extends PropertyIndexQuery {
        private final CoordinateReferenceSystem crs;
        private final PointValue from;
        private final PointValue to;
        private final boolean inclusive;

        private BoundingBoxPredicate(int propertyKeyId, PointValue from, PointValue to, boolean inclusive) {
            super(propertyKeyId);
            // The only user of this predicate is Cypher's BB function,
            // which does not allow null 'corners'.
            Objects.requireNonNull(from);
            Objects.requireNonNull(to);
            this.crs = from.getCoordinateReferenceSystem();
            this.from = from;
            this.to = to;
            this.inclusive = inclusive;
        }

        private BoundingBoxPredicate(int propertyKeyId, PointValue from, PointValue to) {
            this(propertyKeyId, from, to, true);
        }

        @Override
        public boolean acceptsValue(Value value) {
            if (!(value instanceof final PointValue point)) {
                return false;
            }

            CoordinateReferenceSystem crs = point.getCoordinateReferenceSystem();
            if (!crs.equals(this.crs)) {
                return false;
            }

            return crs.getCalculator().withinBBox(point, from, to, inclusive);
        }

        @Override
        public ValueGroup valueGroup() {
            return ValueGroup.GEOMETRY;
        }

        public CoordinateReferenceSystem crs() {
            return crs;
        }

        public PointValue from() {
            return from;
        }

        public PointValue to() {
            return to;
        }

        @Override
        public IndexQueryType type() {
            return IndexQueryType.BOUNDING_BOX;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            BoundingBoxPredicate that = (BoundingBoxPredicate) o;
            return inclusive == that.inclusive
                    && crs == that.crs
                    && Objects.equals(from, that.from)
                    && Objects.equals(to, that.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), crs, from, to, inclusive);
        }
    }

    /**
     * Some value types and values are defined as incomparable and range seek must always return empty result for any range of those types.
     * This is how the behaviour is defined by the Cypher value spec.
     * <p>
     * Incomparable types and values:
     * <ul>
     *     <li>{@link ValueGroup#DURATION}</li>
     *     <li>{@link ValueGroup#DURATION_ARRAY}</li>
     *     <li>{@link ValueGroup#GEOMETRY}</li>
     *     <li>{@link ValueGroup#GEOMETRY_ARRAY}</li>
     *     <li>{@link Values#NaN}</li>
     * </ul>
     */
    public static class IncomparableRangePredicate<T extends Value> extends RangePredicate<Value> {
        private IncomparableRangePredicate(
                int propertyKeyId, ValueGroup valueGroup, T from, boolean fromInclusive, T to, boolean toInclusive) {
            super(propertyKeyId, valueGroup, from, fromInclusive, to, toInclusive);
        }

        @Override
        public boolean acceptsValue(Value value) {
            return false;
        }
    }

    /**
     * Some values are defined as incomparable and exact seek must always return empty result for those predicates.
     * This is how the behaviour is defined by the Cypher value spec.
     * <p>
     * Incomparable values:
     * <ul>
     *     <li>{@link Values#NaN}</li>
     * </ul>
     */
    public static class IncomparableExactPredicate extends ExactPredicate {
        private IncomparableExactPredicate(int propertyKeyId, Value value) {
            super(propertyKeyId, value);
        }

        @Override
        public boolean acceptsValue(Value value) {
            return false;
        }
    }

    public abstract static class StringPredicate extends PropertyIndexQuery {
        private StringPredicate(int propertyKeyId) {
            super(propertyKeyId);
        }

        @Override
        public ValueGroup valueGroup() {
            return ValueGroup.TEXT;
        }

        protected static TextValue asUTF8StringValue(TextValue in) {
            if (in instanceof UTF8StringValue) {
                return in;
            } else {
                return utf8Value(in.stringValue().getBytes(UTF_8));
            }
        }
    }

    public static final class StringPrefixPredicate extends StringPredicate {
        private final TextValue prefix;

        private StringPrefixPredicate(int propertyKeyId, TextValue prefix) {
            super(propertyKeyId);
            // we know utf8 values are coming from the index so optimize for that
            this.prefix = asUTF8StringValue(prefix);
        }

        @Override
        public IndexQueryType type() {
            return IndexQueryType.STRING_PREFIX;
        }

        @Override
        public boolean acceptsValue(Value value) {
            return Values.isTextValue(value) && ((TextValue) value).startsWith(prefix);
        }

        public TextValue prefix() {
            return prefix;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            StringPrefixPredicate that = (StringPrefixPredicate) o;
            return Objects.equals(prefix, that.prefix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), prefix);
        }
    }

    public static final class StringContainsPredicate extends StringPredicate {
        private final TextValue contains;

        private StringContainsPredicate(int propertyKeyId, TextValue contains) {
            super(propertyKeyId);
            // we know utf8 values are coming from the index so optimize for that
            this.contains = asUTF8StringValue(contains);
        }

        @Override
        public IndexQueryType type() {
            return IndexQueryType.STRING_CONTAINS;
        }

        @Override
        public boolean acceptsValue(Value value) {
            return Values.isTextValue(value) && ((TextValue) value).contains(contains);
        }

        public TextValue contains() {
            return contains;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            StringContainsPredicate that = (StringContainsPredicate) o;
            return Objects.equals(contains, that.contains);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), contains);
        }
    }

    public static final class StringSuffixPredicate extends StringPredicate {
        private final TextValue suffix;

        private StringSuffixPredicate(int propertyKeyId, TextValue suffix) {
            super(propertyKeyId);
            // we know utf8 values are coming from the index so optimize for that
            this.suffix = asUTF8StringValue(suffix);
        }

        @Override
        public IndexQueryType type() {
            return IndexQueryType.STRING_SUFFIX;
        }

        @Override
        public boolean acceptsValue(Value value) {
            return Values.isTextValue(value) && ((TextValue) value).endsWith(suffix);
        }

        public TextValue suffix() {
            return suffix;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            StringSuffixPredicate that = (StringSuffixPredicate) o;
            return Objects.equals(suffix, that.suffix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), suffix);
        }
    }

    public static final class FulltextSearchPredicate extends StringPredicate {
        private final String query;
        private final String queryAnalyzer;

        private FulltextSearchPredicate(String query) {
            this(query, null);
        }

        private FulltextSearchPredicate(String query, String queryAnalyzer) {
            super(TokenRead.NO_TOKEN);
            this.query = query;
            this.queryAnalyzer = queryAnalyzer;
        }

        @Override
        public IndexQueryType type() {
            return IndexQueryType.FULLTEXT_SEARCH;
        }

        @Override
        public boolean acceptsValue(Value value) {
            throw new UnsupportedOperationException(
                    "Fulltext search predicates do not know how to evaluate themselves.");
        }

        public String query() {
            return query;
        }

        public String queryAnalyzer() {
            return queryAnalyzer;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            FulltextSearchPredicate that = (FulltextSearchPredicate) o;
            return Objects.equals(query, that.query) && Objects.equals(queryAnalyzer, that.queryAnalyzer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), query, queryAnalyzer);
        }
    }

    public static final class NearestNeighborsPredicate extends PropertyIndexQuery {
        private final int k;
        private final float[] query;

        private NearestNeighborsPredicate(int k, float... query) {
            super(TokenRead.NO_TOKEN);
            this.k = k;
            this.query = query;
        }

        @Override
        public boolean acceptsValue(Value value) {
            throw new UnsupportedOperationException(
                    "Nearest neighbour predicates do not know how to evaluate themselves.");
        }

        @Override
        public ValueGroup valueGroup() {
            return ValueGroup.NUMBER_ARRAY;
        }

        @Override
        public IndexQueryType type() {
            return IndexQueryType.NEAREST_NEIGHBORS;
        }

        public int numberOfNeighbors() {
            return k;
        }

        public float[] query() {
            return query;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            NearestNeighborsPredicate that = (NearestNeighborsPredicate) o;
            return k == that.k && Arrays.equals(query, that.query);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(super.hashCode(), k);
            result = 31 * result + Arrays.hashCode(query);
            return result;
        }
    }
}
