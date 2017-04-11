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
package org.neo4j.kernel.api.schema;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.function.Predicate;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.PropertyValueComparison;

public abstract class IndexQuery implements Predicate<Object>
{
    /**
     * Searches the index for all entries that has the given property.
     *
     * @param propertyKeyId the property ID to match.
     * @return an {@link IndexQuery} instance to be passed to {@link ReadOperations#indexQuery(IndexDescriptor,
     * IndexQuery...)}
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
     * @return an {@link IndexQuery} instance to be passed to {@link ReadOperations#indexQuery(IndexDescriptor,
     * IndexQuery...)}
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
     * @return an {@link IndexQuery} instance to be passed to {@link ReadOperations#indexQuery(IndexDescriptor,
     * IndexQuery...)}
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
     * @return an {@link IndexQuery} instance to be passed to {@link ReadOperations#indexQuery(IndexDescriptor,
     * IndexQuery...)}
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
     * @return an {@link IndexQuery} instance to be passed to {@link ReadOperations#indexQuery(IndexDescriptor,
     * IndexQuery...)}
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
     * @return an {@link IndexQuery} instance to be passed to {@link ReadOperations#indexQuery(IndexDescriptor,
     * IndexQuery...)}
     */
    public static StringContainsPredicate stringContains( int propertyKeyId, String contains )
    {
        return  new StringContainsPredicate( propertyKeyId, contains );
    }

    /**
     * Searches the index string values ending with {@code suffix}.
     *
     * @param propertyKeyId the property ID to match.
     * @param suffix the string suffix to search for.
     * @return an {@link IndexQuery} instance to be passed to {@link ReadOperations#indexQuery(IndexDescriptor,
     * IndexQuery...)}
     */
    public static StringSuffixPredicate stringSuffix( int propertyKeyId, String suffix )
    {
        return new StringSuffixPredicate( propertyKeyId, suffix );
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

    public abstract boolean test( Object value );

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
        public boolean test( Object value )
        {
            return value != null;
        }
    }

    public static final class ExactPredicate extends IndexQuery
    {
        private final DefinedProperty exactValue;

        ExactPredicate( int propertyKeyId, Object value )
        {
            super( propertyKeyId );
            this.exactValue = Property.property( propertyKeyId, value );
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.exact;
        }

        @Override
        public boolean test( Object value )
        {
            return exactValue.valueEquals( value );
        }

        public Object value()
        {
            return exactValue.value();
        }
    }

    public static final class NumberRangePredicate extends IndexQuery
    {
        private final Number from;
        private final boolean fromInclusive;
        private final Number to;
        private final boolean toInclusive;

        NumberRangePredicate( int propertyKeyId, Number from, boolean fromInclusive, Number to, boolean toInclusive )
        {
            super( propertyKeyId );
            this.from = from;
            this.fromInclusive = fromInclusive;
            this.to = to;
            this.toInclusive = toInclusive;
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.rangeNumeric;
        }

        @Override
        public boolean test( Object value )
        {
            if ( value == null )
            {
                return false;
            }
            if ( !(value instanceof Number) )
            {
                return false;
            }
            Number number = (Number) value;
            if ( from != null )
            {
                int compare = PropertyValueComparison.COMPARE_NUMBERS.compare( number, from );
                if ( compare < 0 || !fromInclusive && compare == 0 )
                {
                    return false;
                }
            }
            if ( to != null )
            {
                int compare = PropertyValueComparison.COMPARE_NUMBERS.compare( number, to );
                if ( compare > 0 || !toInclusive && compare == 0 )
                {
                    return false;
                }
            }
            return true;
        }

        public Number from()
        {
            return from;
        }

        public Number to()
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

    public static final class StringRangePredicate extends IndexQuery
    {
        private final String from;
        private final boolean fromInclusive;
        private final String to;
        private final boolean toInclusive;

        StringRangePredicate( int propertyKeyId, String from, boolean fromInclusive, String to, boolean toInclusive )
        {
            super( propertyKeyId );
            this.from = from;
            this.fromInclusive = fromInclusive;
            this.to = to;
            this.toInclusive = toInclusive;
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.rangeString;
        }

        @Override
        public boolean test( Object value )
        {
            if ( value == null )
            {
                return false;
            }
            if ( !(value instanceof String) )
            {
                return false;
            }
            String str = (String) value;
            if ( from != null )
            {
                int compare = PropertyValueComparison.COMPARE_STRINGS.compare( str, from );
                if ( compare < 0 || !fromInclusive && compare == 0 )
                {
                    return false;
                }
            }
            if ( to != null )
            {
                int compare = PropertyValueComparison.COMPARE_STRINGS.compare( str, to );
                if ( compare > 0 || !toInclusive && compare == 0 )
                {
                    return false;
                }
            }
            return true;
        }

        public String from()
        {
            return from;
        }

        public boolean fromInclusive()
        {
            return fromInclusive;
        }

        public String to()
        {
            return to;
        }

        public boolean toInclusive()
        {
            return toInclusive;
        }
    }

    public static final class StringPrefixPredicate extends IndexQuery
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
        public boolean test( Object value )
        {
            return value != null && value instanceof String && ((String)value).startsWith( prefix );
        }

        public String prefix()
        {
            return prefix;
        }
    }

    public static final class StringContainsPredicate extends IndexQuery
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
        public boolean test( Object value )
        {
            return value != null && value instanceof String && ((String)value).contains( contains );
        }

        public String contains()
        {
            return contains;
        }
    }

    public static final class StringSuffixPredicate extends IndexQuery
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
        public boolean test( Object value )
        {
            return value != null && value instanceof String && ((String)value).endsWith( suffix );
        }

        public String suffix()
        {
            return suffix;
        }
    }
}
