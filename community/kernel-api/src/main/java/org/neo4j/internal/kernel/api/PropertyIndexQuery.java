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
package org.neo4j.internal.kernel.api;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.neo4j.internal.schema.IndexQuery;
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNullElse;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.utf8Value;

public abstract class PropertyIndexQuery implements IndexQuery
{
    /**
     * Searches the index for all entries that has the given property.
     *
     * @param propertyKeyId the property ID to match.
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static ExistsPredicate exists( int propertyKeyId )
    {
        return new ExistsPredicate( propertyKeyId );
    }

    /**
     * Searches the index for a certain value.
     *
     * @param propertyKeyId the property ID to match.
     * @param value the property value to search for.
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static ExactPredicate exact( int propertyKeyId, Object value )
    {
        return new ExactPredicate( propertyKeyId, value );
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
    public static RangePredicate<?> range( int propertyKeyId,
                                           Number from, boolean fromInclusive,
                                           Number to, boolean toInclusive )
    {
        return NumberRangePredicate.create( propertyKeyId,
                                            from == null ? null : Values.numberValue( from ), fromInclusive,
                                            to == null ? null : Values.numberValue( to ), toInclusive );
    }

    public static RangePredicate<?> range( int propertyKeyId,
                                           String from, boolean fromInclusive,
                                           String to, boolean toInclusive )
    {
        return new TextRangePredicate( propertyKeyId,
                                       from == null ? null : Values.stringValue( from ), fromInclusive,
                                       to == null ? null : Values.stringValue( to ), toInclusive );
    }

    public static <VALUE extends Value> RangePredicate<?> range( int propertyKeyId,
                                                                 VALUE from, boolean fromInclusive,
                                                                 VALUE to, boolean toInclusive )
    {
        if ( from == null && to == null )
        {
            throw new IllegalArgumentException( "Cannot create RangePredicate without at least one bound" );
        }

        ValueGroup valueGroup = requireNonNullElse( from, to ).valueGroup();
        switch ( valueGroup )
        {
        case NUMBER:
            return NumberRangePredicate.create( propertyKeyId,
                                                (NumberValue) from, fromInclusive,
                                                (NumberValue) to, toInclusive );

        case TEXT:
            return new TextRangePredicate( propertyKeyId,
                                           (TextValue) from, fromInclusive,
                                           (TextValue) to, toInclusive );

        case GEOMETRY:
            return new GeometryRangePredicate( propertyKeyId,
                                               (PointValue) from, fromInclusive,
                                               (PointValue) to, toInclusive );

        default:
            return new RangePredicate<>( propertyKeyId, valueGroup, from, fromInclusive, to, toInclusive );
        }
    }

    /**
     * Create IndexQuery for retrieving all indexed entries of the given value group.
     */
    public static RangePredicate<?> range( int propertyKeyId, ValueGroup valueGroup )
    {
        if ( valueGroup == ValueGroup.GEOMETRY )
        {
            throw new IllegalArgumentException( "Cannot create GeometryRangePredicate without a specified CRS" );
        }
        return new RangePredicate<>( propertyKeyId, valueGroup );
    }

    /**
     * Create IndexQuery for retrieving all indexed entries with spatial value of the given
     * coordinate reference system.
     */
    public static RangePredicate<?> range( int propertyKeyId, CoordinateReferenceSystem crs )
    {
        return new GeometryRangePredicate( propertyKeyId, crs );
    }

    /**
     * Searches the index string values starting with {@code prefix}.
     *
     * @param propertyKeyId the property ID to match.
     * @param prefix the string prefix to search for.
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static StringPrefixPredicate stringPrefix( int propertyKeyId, TextValue prefix )
    {
        return new StringPrefixPredicate( propertyKeyId, prefix );
    }

    /**
     * Searches the index for string values containing the exact search string.
     *
     * @param propertyKeyId the property ID to match.
     * @param contains the string to search for.
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static StringContainsPredicate stringContains( int propertyKeyId, TextValue contains )
    {
        return new StringContainsPredicate( propertyKeyId, contains );
    }

    /**
     * Searches the index string values ending with {@code suffix}.
     *
     * @param propertyKeyId the property ID to match.
     * @param suffix the string suffix to search for.
     * @return an {@link PropertyIndexQuery} instance to be used for querying an index.
     */
    public static StringSuffixPredicate stringSuffix( int propertyKeyId, TextValue suffix )
    {
        return new StringSuffixPredicate( propertyKeyId, suffix );
    }

    public static PropertyIndexQuery fulltextSearch( String query )
    {
        return new FulltextSearchPredicate( query );
    }

    public static ValueTuple asValueTuple( PropertyIndexQuery.ExactPredicate... query )
    {
        Value[] values = new Value[query.length];
        for ( int i = 0; i < query.length; i++ )
        {
            values[i] = query[i].value();
        }
        return ValueTuple.of( values );
    }

    private final int propertyKeyId;

    protected PropertyIndexQuery( int propertyKeyId )
    {
        this.propertyKeyId = propertyKeyId;
    }

    @SuppressWarnings( "EqualsWhichDoesntCheckParameterClass" )
    @Override
    public final boolean equals( Object other )
    {
        // equals() and hashcode() are only used for testing so we don't care that they are a bit slow.
        return EqualsBuilder.reflectionEquals( this, other );
    }

    @Override
    public final int hashCode()
    {
        // equals() and hashcode() are only used for testing so we don't care that they are a bit slow.
        return HashCodeBuilder.reflectionHashCode( this, false );
    }

    @Override
    public final String toString()
    {
        // Only used to debugging, it's okay to be a bit slow.
        return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
    }

    /**
     * @return The ID of the property key, this queries against.
     */
    public final int propertyKeyId()
    {
        return propertyKeyId;
    }

    @Override
    public final int queriedId()
    {
        return propertyKeyId();
    }

    /**
     * @param value to test against the query.
     * @return true if the {@code value} satisfies the query; false otherwise.
     */
    public abstract boolean acceptsValue( Value value );

    public boolean acceptsValueAt( PropertyCursor property )
    {
        return acceptsValue( property.propertyValue() );
    }

    /**
     * @return Target {@link ValueGroup} for query or {@link ValueGroup#UNKNOWN} if not targeting single group.
     */
    public abstract ValueGroup valueGroup();

    /**
     * @return Target {@link ValueCategory} for query
     */
    public ValueCategory valueCategory()
    {
        return valueGroup().category();
    }

    public static final class ExistsPredicate extends PropertyIndexQuery
    {
        private ExistsPredicate( int propertyKeyId )
        {
            super( propertyKeyId );
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.EXISTS;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            return value != null && value != NO_VALUE;
        }

        @Override
        public boolean acceptsValueAt( PropertyCursor property )
        {
            return true;
        }

        @Override
        public ValueGroup valueGroup()
        {
            return ValueGroup.UNKNOWN;
        }
    }

    public static final class ExactPredicate extends PropertyIndexQuery
    {
        private final Value exactValue;

        private ExactPredicate( int propertyKeyId, Object value )
        {
            super( propertyKeyId );
            this.exactValue = value instanceof Value ? (Value)value : Values.of( value );
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.EXACT;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            return exactValue.equals( value );
        }

        @Override
        public ValueGroup valueGroup()
        {
            return exactValue.valueGroup();
        }

        public Value value()
        {
            return exactValue;
        }
    }

    public static class RangePredicate<T extends Value> extends PropertyIndexQuery
    {
        protected final T from;
        protected final boolean fromInclusive;
        protected final T to;
        protected final boolean toInclusive;
        protected final ValueGroup valueGroup;

        RangePredicate( int propertyKeyId, ValueGroup valueGroup,
                        T from, boolean fromInclusive,
                        T to, boolean toInclusive )
        {
            super( propertyKeyId );
            this.valueGroup = valueGroup;
            this.from = from;
            this.fromInclusive = fromInclusive;
            this.to = to;
            this.toInclusive = toInclusive;
        }

        RangePredicate( int propertyKeyId, ValueGroup valueGroup )
        {
            this( propertyKeyId, valueGroup, null, true, null, true );
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.RANGE;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            if ( value == null || value == NO_VALUE || value.valueGroup() != valueGroup )
            {
                return false;
            }
            if ( from != null )
            {
                int compare = Values.COMPARATOR.compare( value, from );
                if ( compare < 0 || !fromInclusive && compare == 0 )
                {
                    return false;
                }
            }
            if ( to != null )
            {
                int compare = Values.COMPARATOR.compare( value, to );
                return compare <= 0 && (toInclusive || compare != 0);
            }
            return true;
        }

        @Override
        public ValueGroup valueGroup()
        {
            return valueGroup;
        }

        public Value fromValue()
        {
            return from == null ? NO_VALUE : from;
        }

        public Value toValue()
        {
            return to == null ? NO_VALUE : to;
        }

        public boolean fromInclusive()
        {
            return fromInclusive;
        }

        public boolean toInclusive()
        {
            return toInclusive;
        }

        /**
         * @return true if the order defined for this type can also be relied on for bounds comparisons.
         */
        public boolean isRegularOrder()
        {
            return true;
        }
    }

    public static final class GeometryRangePredicate extends RangePredicate<PointValue>
    {
        private final CoordinateReferenceSystem crs;

        private GeometryRangePredicate( int propertyKeyId, CoordinateReferenceSystem crs,
                                        PointValue from, boolean fromInclusive,
                                        PointValue to, boolean toInclusive )
        {
            super( propertyKeyId, ValueGroup.GEOMETRY, from, fromInclusive, to, toInclusive );
            this.crs = crs;
        }

        private GeometryRangePredicate( int propertyKeyId,
                                        PointValue from, boolean fromInclusive,
                                        PointValue to, boolean toInclusive )
        {
            this( propertyKeyId, requireNonNullElse( from, to ).getCoordinateReferenceSystem(),
                  from, fromInclusive, to, toInclusive );
        }

        private GeometryRangePredicate( int propertyKeyId, CoordinateReferenceSystem crs )
        {
            this( propertyKeyId, crs, null, true, null, true );
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            if ( !(value instanceof PointValue) )
            {
                return false;
            }

            final var point = (PointValue) value;
            if ( !point.getCoordinateReferenceSystem().equals( crs ) )
            {
                return false;
            }

            final var within = point.withinRange( from, fromInclusive, to, toInclusive );
            return within != null && within;
        }

        public CoordinateReferenceSystem crs()
        {
            return crs;
        }

        public PointValue from()
        {
            return from;
        }

        public PointValue to()
        {
            return to;
        }

        /**
         * The order defined for spatial types cannot be used for bounds comparisons.
         * @return false
         */
        @Override
        public boolean isRegularOrder()
        {
            return false;
        }
    }

    public static final class TextRangePredicate extends RangePredicate<TextValue>
    {
        private TextRangePredicate( int propertyKeyId, TextValue from, boolean fromInclusive, TextValue to,
                boolean toInclusive )
        {
            super( propertyKeyId, ValueGroup.TEXT, from, fromInclusive, to, toInclusive );
        }

        public String from()
        {
            return from == null ? null : from.stringValue();
        }

        public String to()
        {
            return to == null ? null : to.stringValue();
        }
    }

    public abstract static class StringPredicate extends PropertyIndexQuery
    {
        private StringPredicate( int propertyKeyId )
        {
            super( propertyKeyId );
        }

        @Override
        public ValueGroup valueGroup()
        {
            return ValueGroup.TEXT;
        }

        protected static TextValue asUTF8StringValue( TextValue in )
        {
            if ( in instanceof UTF8StringValue )
            {
                return in;
            }
            else
            {
                return utf8Value( in.stringValue().getBytes( UTF_8 ) );
            }
        }
    }

    public static final class StringPrefixPredicate extends StringPredicate
    {
        private final TextValue prefix;

        private StringPrefixPredicate( int propertyKeyId, TextValue prefix )
        {
            super( propertyKeyId );
            //we know utf8 values are coming from the index so optimize for that
            this.prefix = asUTF8StringValue( prefix );
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.STRING_PREFIX;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            return Values.isTextValue( value ) && ((TextValue) value).startsWith( prefix );
        }

        public TextValue prefix()
        {
            return prefix;
        }
    }

    public static final class StringContainsPredicate extends StringPredicate
    {
        private final TextValue contains;

        private StringContainsPredicate( int propertyKeyId, TextValue contains )
        {
            super( propertyKeyId );
            //we know utf8 values are coming from the index so optimize for that
            this.contains = asUTF8StringValue( contains );
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.STRING_CONTAINS;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            return Values.isTextValue( value ) && ((TextValue) value).contains( contains );
        }

        public TextValue contains()
        {
            return contains;
        }
    }

    public static final class StringSuffixPredicate extends StringPredicate
    {
        private final TextValue suffix;

        private StringSuffixPredicate( int propertyKeyId, TextValue suffix )
        {
            super( propertyKeyId );
            //we know utf8 values are coming from the index so optimize for that
            this.suffix = asUTF8StringValue( suffix );
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.STRING_SUFFIX;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            return Values.isTextValue( value ) && ((TextValue) value).endsWith( suffix );
        }

        public TextValue suffix()
        {
            return suffix;
        }
    }

    public static final class FulltextSearchPredicate extends StringPredicate
    {
        private final String query;

        private FulltextSearchPredicate( String query )
        {
            super( TokenRead.NO_TOKEN );
            this.query = query;
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.FULLTEXT_SEARCH;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            throw new UnsupportedOperationException( "Fulltext search predicates do not know how to evaluate themselves." );
        }

        public String query()
        {
            return query;
        }
    }
}
