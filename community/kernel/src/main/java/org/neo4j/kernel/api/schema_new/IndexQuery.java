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
package org.neo4j.kernel.api.schema_new;

public abstract class IndexQuery
{
    public static ExistsPredicate exists( int propertyKeyId )
    {
        return new ExistsPredicate( propertyKeyId );
    }

    public static ExactPredicate exact( int propertyKeyId, Object value )
    {
        return new ExactPredicate( propertyKeyId, value );
    }

    public static NumberRangePredicate range( int propertyKeyId, Number from, boolean fromInclusive, Number to,
                                              boolean toInclusive )
    {
        return new NumberRangePredicate( propertyKeyId, from, fromInclusive, to, toInclusive );
    }

    public static StringRangePredicate range( int propertyKeyId, String from, boolean fromInclusive, String to,
                                              boolean toInclusive )
    {
        return new StringRangePredicate( propertyKeyId, from, fromInclusive, to, toInclusive );
    }

    public abstract IndexQueryType type();

    public static final class ExistsPredicate extends IndexQuery
    {
        private final int propertyKeyId;

        public ExistsPredicate( int propertyKeyId )
        {
            this.propertyKeyId = propertyKeyId;
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.exists;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            ExistsPredicate that = (ExistsPredicate) o;

            return propertyKeyId == that.propertyKeyId;
        }

        @Override
        public int hashCode()
        {
            return propertyKeyId;
        }
    }

    public static final class ExactPredicate extends IndexQuery
    {
        private final int propertyKeyId;
        private final Object value;

        public ExactPredicate( int propertyKeyId, Object value )
        {
            this.propertyKeyId = propertyKeyId;
            this.value = value;
        }

        @Override
        public IndexQueryType type()
        {
            return IndexQueryType.exact;
        }

        public Object value()
        {
            return value;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            ExactPredicate that = (ExactPredicate) o;

            if ( propertyKeyId != that.propertyKeyId )
            { return false; }
            return value.equals( that.value );
        }

        @Override
        public int hashCode()
        {
            int result = propertyKeyId;
            result = 31 * result + value.hashCode();
            return result;
        }
    }

    public static final class NumberRangePredicate extends IndexQuery
    {
        private final int propertyKeyId;
        private final Number from;
        private final boolean fromInclusive;
        private final Number to;
        private final boolean toInclusive;

        public NumberRangePredicate( int propertyKeyId, Number from,
                                     boolean fromInclusive, Number to, boolean toInclusive )
        {
            this.propertyKeyId = propertyKeyId;
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

        public Number getFrom()
        {
            return from;
        }

        public Number getTo()
        {
            return to;
        }

        public boolean isFromInclusive()
        {
            return fromInclusive;
        }

        public boolean isToInclusive()
        {
            return toInclusive;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            NumberRangePredicate that = (NumberRangePredicate) o;

            if ( propertyKeyId != that.propertyKeyId )
            { return false; }
            if ( fromInclusive != that.fromInclusive )
            { return false; }
            if ( toInclusive != that.toInclusive )
            { return false; }
            if ( !from.equals( that.from ) )
            { return false; }
            return to.equals( that.to );
        }

        @Override
        public int hashCode()
        {
            int result = propertyKeyId;
            result = 31 * result + from.hashCode();
            result = 31 * result + (fromInclusive ? 1 : 0);
            result = 31 * result + to.hashCode();
            result = 31 * result + (toInclusive ? 1 : 0);
            return result;
        }
    }

    public static final class StringRangePredicate extends IndexQuery
    {
        private final int propertyKeyId;
        private final String from;
        private final boolean fromInclusive;
        private final String to;
        private final boolean toInclusive;

        public StringRangePredicate( int propertyKeyId, String from,
                                     boolean fromInclusive, String to,
                                     boolean toInclusive )
        {
            this.propertyKeyId = propertyKeyId;
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

        public String getFrom()
        {
            return from;
        }

        public boolean isFromInclusive()
        {
            return fromInclusive;
        }

        public String getTo()
        {
            return to;
        }

        public boolean isToInclusive()
        {
            return toInclusive;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            StringRangePredicate that = (StringRangePredicate) o;

            if ( propertyKeyId != that.propertyKeyId )
            { return false; }
            if ( fromInclusive != that.fromInclusive )
            { return false; }
            if ( toInclusive != that.toInclusive )
            { return false; }
            if ( !from.equals( that.from ) )
            { return false; }
            return to.equals( that.to );
        }

        @Override
        public int hashCode()
        {
            int result = propertyKeyId;
            result = 31 * result + from.hashCode();
            result = 31 * result + (fromInclusive ? 1 : 0);
            result = 31 * result + to.hashCode();
            result = 31 * result + (toInclusive ? 1 : 0);
            return result;
        }
    }

    public enum IndexQueryType
    {
        exists,
        exact,
        rangeString,
        rangeNumeric
    }
}
