/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

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
     * @param from the lower bound of the property value.
     * @param fromInclusive the lower bound is inclusive if true.
     * @param to the upper bound of the property value.
     * @param toInclusive the upper bound is inclusive if true.
     * @return an {@link IndexQuery} instance to be used for querying an index.
     */
    public static NumberRangePredicate range( int propertyKeyId, Number from, boolean fromInclusive, Number to,
                                              boolean toInclusive )
    {
        return new NumberRangePredicate( propertyKeyId, from, fromInclusive, to, toInclusive );
    }

    /**
     * Searches the index for string values between {@code from} and {@code to}.
     *
     * @param propertyKeyId the property ID to match.
     * @param from the lower bound of the property value.
     * @param fromInclusive the lower bound is inclusive if true.
     * @param to the upper bound of the property value.
     * @param toInclusive the upper bound is inclusive if true.
     * @return an {@link IndexQuery} instance to be used for querying an index.
     */
    public static StringRangePredicate range( int propertyKeyId, String from, boolean fromInclusive, String to,
                                              boolean toInclusive )
    {
        return new StringRangePredicate( propertyKeyId, from, fromInclusive, to, toInclusive );
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
        rangeString,
        rangeNumeric,
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
            return value != null && value != Values.NO_VALUE;
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

    public static final class NumberRangePredicate extends IndexQuery
    {
        private final Value from;
        private final boolean fromInclusive;
        private final Value to;
        private final boolean toInclusive;

        NumberRangePredicate( int propertyKeyId, Number from, boolean fromInclusive, Number to, boolean toInclusive )
        {
            super( propertyKeyId );
            this.from = Values.numberValue( from );
            this.fromInclusive = fromInclusive;
            this.to = Values.numberValue( to );
            this.toInclusive = toInclusive;
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.rangeNumeric;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            if ( value == null )
            {
                return false;
            }
            if ( Values.isNumberValue( value ) )
            {
                if ( from != Values.NO_VALUE )
                {
                    int compare = Values.COMPARATOR.compare( value, from );
                    if ( compare < 0 || !fromInclusive && compare == 0 )
                    {
                        return false;
                    }
                }
                if ( to != Values.NO_VALUE )
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
            return ValueGroup.NUMBER;
        }

        public Number from()
        {
            return (Number)from.asObject();
        }

        public Number to()
        {
            return (Number)to.asObject();
        }

        public Value fromAsValue()
        {
            return from;
        }

        public Value toAsValue()
        {
            return to;
        }

        public boolean fromInclusive()
        {
            return fromInclusive;
        }

        public boolean toInclusive()
        {
            return toInclusive;
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

    public static final class StringRangePredicate extends StringPredicate
    {
        private final Value from;
        private final boolean fromInclusive;
        private final Value to;
        private final boolean toInclusive;

        StringRangePredicate( int propertyKeyId, String from, boolean fromInclusive, String to, boolean toInclusive )
        {
            super( propertyKeyId );
            this.from = Values.stringOrNoValue( from );
            this.fromInclusive = fromInclusive;
            this.to = Values.stringOrNoValue( to );
            this.toInclusive = toInclusive;
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.rangeString;
        }

        @Override
        public boolean acceptsValue( Value value )
        {
            if ( value == null )
            {
                return false;
            }
            if ( !Values.isTextValue( value ) )
            {
                return false;
            }
            if ( from != Values.NO_VALUE )
            {
                int compare = Values.COMPARATOR.compare( value, from );
                if ( compare < 0 || !fromInclusive && compare == 0 )
                {
                    return false;
                }
            }
            if ( to != Values.NO_VALUE )
            {
                int compare = Values.COMPARATOR.compare( value, to );
                if ( compare > 0 || !toInclusive && compare == 0 )
                {
                    return false;
                }
            }
            return true;
        }

        public String from()
        {
            return (String)from.asObject();
        }

        public boolean fromInclusive()
        {
            return fromInclusive;
        }

        public String to()
        {
            return (String)to.asObject();
        }

        public boolean toInclusive()
        {
            return toInclusive;
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
            return value != null && Values.isTextValue( value ) &&
                    ((TextValue)value).stringValue().startsWith( prefix );
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
            return value != null && Values.isTextValue( value ) && ((String)value.asObject()).contains( contains );
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
            return value != null && Values.isTextValue( value ) && ((String)value.asObject()).endsWith( suffix );
        }

        public String suffix()
        {
            return suffix;
        }
    }
}
