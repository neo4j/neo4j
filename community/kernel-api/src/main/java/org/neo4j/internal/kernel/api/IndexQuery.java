/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.Values.NO_VALUE;

public abstract class IndexQuery
{
    /**
     * Searches the index for all entries that has the given property.
     *
     * @param propertyKeyId the property ID to match.
     * @return an {@link IndexQuery} instance to be used for querying an index.
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
     * @return an {@link IndexQuery} instance to be used for querying an index.
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
     * @return an {@link IndexQuery} instance to be used for querying an index.
     */
    public static RangePredicate<?> range( int propertyKeyId,
                                        Number from, boolean fromInclusive,
                                        Number to, boolean toInclusive )
    {
        return new NumberRangePredicate( propertyKeyId,
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

        ValueGroup valueGroup = from != null ? from.valueGroup() : to.valueGroup();
        switch ( valueGroup )
        {
        case NUMBER:
            return new NumberRangePredicate( propertyKeyId,
                                             (NumberValue)from, fromInclusive,
                                             (NumberValue)to, toInclusive );

        case TEXT:
            return new TextRangePredicate( propertyKeyId,
                                           (TextValue)from, fromInclusive,
                                           (TextValue)to, toInclusive );

        case GEOMETRY:
            PointValue pFrom = (PointValue)from;
            PointValue pTo = (PointValue)to;
            CoordinateReferenceSystem crs = pFrom != null ? pFrom.getCoordinateReferenceSystem() : pTo.getCoordinateReferenceSystem();
            return new GeometryRangePredicate( propertyKeyId, crs, pFrom, fromInclusive, pTo, toInclusive );

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
        return new RangePredicate<>( propertyKeyId, valueGroup, null, true, null, true );
    }

    /**
     * Create IndexQuery for retrieving all indexed entries with spatial value of the given
     * coordinate reference system.
     */
    public static RangePredicate<?> range( int propertyKeyId, CoordinateReferenceSystem crs )
    {
        return new GeometryRangePredicate( propertyKeyId, crs, null, true, null, true );
    }

    /**
     * Searches the index string values starting with {@code prefix}.
     *
     * @param propertyKeyId the property ID to match.
     * @param prefix the string prefix to search for.
     * @return an {@link IndexQuery} instance to be used for querying an index.
     */
    public static StringPrefixPredicate stringPrefix( int propertyKeyId, String prefix )
    {
        return new StringPrefixPredicate( propertyKeyId, prefix );
    }

    /**
     * Searches the index for string values containing the exact search string.
     *
     * @param propertyKeyId the property ID to match.
     * @param contains the string to search for.
     * @return an {@link IndexQuery} instance to be used for querying an index.
     */
    public static StringContainsPredicate stringContains( int propertyKeyId, String contains )
    {
        return new StringContainsPredicate( propertyKeyId, contains );
    }

    /**
     * Searches the index string values ending with {@code suffix}.
     *
     * @param propertyKeyId the property ID to match.
     * @param suffix the string suffix to search for.
     * @return an {@link IndexQuery} instance to be used for querying an index.
     */
    public static StringSuffixPredicate stringSuffix( int propertyKeyId, String suffix )
    {
        return new StringSuffixPredicate( propertyKeyId, suffix );
    }

    public static ValueTuple asValueTuple( IndexQuery.ExactPredicate... query )
    {
        Value[] values = new Value[query.length];
        for ( int i = 0; i < query.length; i++ )
        {
            values[i] = query[i].value();
        }
        return ValueTuple.of( values );
    }

    private final int propertyKeyId;

    protected IndexQuery( int propertyKeyId )
    {
        this.propertyKeyId = propertyKeyId;
    }

    public abstract IndexQueryType type();

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

    public final int propertyKeyId()
    {
        return propertyKeyId;
    }

    public abstract boolean acceptsValue( Value value );

    public boolean acceptsValueAt( PropertyCursor property )
    {
        return acceptsValue( property.propertyValue() );
    }

    /**
     * @return Target {@link ValueGroup} for query or {@link ValueGroup#UNKNOWN} if not targeting single group.
     */
    public abstract ValueGroup valueGroup();

    public enum IndexQueryType
    {
        exists,
        exact,
        range,
        stringPrefix,
        stringSuffix,
        stringContains
    }

    public static final class ExistsPredicate extends IndexQuery
    {
        ExistsPredicate( int propertyKeyId )
        {
            super( propertyKeyId );
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.exists;
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

    public static final class ExactPredicate extends IndexQuery
    {
        private final Value exactValue;

        ExactPredicate( int propertyKeyId, Object value )
        {
            super( propertyKeyId );
            this.exactValue = value instanceof Value ? (Value)value : Values.of( value );
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.exact;
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

    public static class RangePredicate<T extends Value> extends IndexQuery
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

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.range;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            if ( value == null || value == NO_VALUE )
            {
                return false;
            }
            if ( value.valueGroup() == valueGroup )
            {
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
                    if ( compare > 0 || !toInclusive && compare == 0 )
                    {
                        return false;
                    }
                }
                return true;
            }
            return false;
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

        GeometryRangePredicate( int propertyKeyId, CoordinateReferenceSystem crs, PointValue from, boolean fromInclusive, PointValue to, boolean toInclusive )
        {
            super( propertyKeyId, ValueGroup.GEOMETRY, from, fromInclusive, to, toInclusive );
            this.crs = crs;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            if ( value == null )
            {
                return false;
            }
            if ( value instanceof PointValue )
            {
                PointValue point = (PointValue) value;
                if ( point.getCoordinateReferenceSystem().equals( crs ) )
                {
                    Boolean within = point.withinRange( from, fromInclusive, to, toInclusive );
                    return within == null ? false : within;
                }
            }
            return false;
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

    public static final class NumberRangePredicate extends RangePredicate<NumberValue>
    {
        NumberRangePredicate( int propertyKeyId, NumberValue from, boolean fromInclusive, NumberValue to,
                boolean toInclusive )
        {
            super( propertyKeyId, ValueGroup.NUMBER, from, fromInclusive, to, toInclusive );
        }

        public Number from()
        {
            return from == null ? null : from.asObject();
        }

        public Number to()
        {
            return to == null ? null : to.asObject();
        }
    }

    public static final class TextRangePredicate extends RangePredicate<TextValue>
    {
        TextRangePredicate( int propertyKeyId, TextValue from, boolean fromInclusive, TextValue to,
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

    public abstract static class StringPredicate extends IndexQuery
    {
        StringPredicate( int propertyKeyId )
        {
            super( propertyKeyId );
        }

        @Override
        public ValueGroup valueGroup()
        {
            return ValueGroup.TEXT;
        }
    }

    public static final class StringPrefixPredicate extends StringPredicate
    {
        private final String prefix;

        StringPrefixPredicate( int propertyKeyId, String prefix )
        {
            super( propertyKeyId );
            this.prefix = prefix;
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.stringPrefix;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            return Values.isTextValue( value ) && ((TextValue) value).stringValue().startsWith( prefix );
        }

        public String prefix()
        {
            return prefix;
        }
    }

    public static final class StringContainsPredicate extends StringPredicate
    {
        private final String contains;

        StringContainsPredicate( int propertyKeyId, String contains )
        {
            super( propertyKeyId );
            this.contains = contains;
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.stringContains;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            return Values.isTextValue( value ) && ((String) value.asObject()).contains( contains );
        }

        public String contains()
        {
            return contains;
        }
    }

    public static final class StringSuffixPredicate extends StringPredicate
    {
        private final String suffix;

        StringSuffixPredicate( int propertyKeyId, String suffix )
        {
            super( propertyKeyId );
            this.suffix = suffix;
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.stringSuffix;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            return Values.isTextValue( value ) && ((String) value.asObject()).endsWith( suffix );
        }

        public String suffix()
        {
            return suffix;
        }
    }
}
