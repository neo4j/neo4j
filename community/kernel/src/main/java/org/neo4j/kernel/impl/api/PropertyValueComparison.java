/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.Comparator;

import org.neo4j.helpers.MathUtil;
import org.neo4j.helpers.ObjectUtil;

import static java.lang.String.format;

public class PropertyValueComparison
{
    public final static Object LOWEST_OBJECT = new Object()
    {
        @Override
        public String toString()
        {
            return "";
        }
    };

    // DO NOT CHANGE the sort order without considering the implications for TxState and lucene!

    // This compares two values that have the same super type according to that super type's comparator
    // Any values that fall under OTHER, are compared by ObjectUtil.toString
    // NULL is not supported
    public final static PropertyValueComparator<Object> COMPARE_VALUES = new AnyPropertyValueComparator();

    public final static PropertyValueComparator<Number> COMPARE_NUMBERS = new NumberPropertyValueComparator();

    public final static PropertyValueComparator<Object> COMPARE_STRINGS = new StringPropertyValueComparator();

    public final static PropertyValueComparator<SuperType> COMPARE_SUPER_TYPE = new PropertyValueSuperTypeComparator();

    public enum SuperType
    {
        OTHER( 0, Limit.inclusive( LOWEST_OBJECT ), Limit.<Object>exclusive( "" ) ),
        STRING( 1, Limit.<Object>inclusive( "" ), Limit.<Object>exclusive( false ) ),
        BOOLEAN( 2, Limit.<Object>inclusive( false ), Limit.<Object>inclusive( true ) ),

        // Keep this last so that Double.NaN is the largest value
        NUMBER( 3, Limit.<Object>inclusive( Double.NEGATIVE_INFINITY ), Limit.<Object>inclusive( Double.NaN ) );

        public final int typeId;
        public final Limit<Object> lowLimit;
        public final Limit<Object> highLimit;

        SuperType( int typeId, Limit<Object> lowLimit, Limit<Object> highLimit )
        {
            this.typeId = typeId;
            this.lowLimit = lowLimit;
            this.highLimit = highLimit;
        }

        public boolean isSuperTypeOf( Object value )
        {
            return this == ofValue( value );
        }

        public static SuperType ofValue( Object value )
        {
            if ( null == value )
            {
                throw new NullPointerException( "null is not a valid property value and hence has no " +
                        "PropertyValueComparison.SuperType" );
            }

            if ( value instanceof String )
            {
                return STRING;
            }
            else if ( value instanceof Number )
            {
                return NUMBER;
            }
            else if ( value instanceof Boolean )
            {
                return BOOLEAN;
            }
            else if ( value instanceof Character )
            {
                return STRING;
            }

            return OTHER;
        }

        public static Comparator<SuperType> TYPE_ID_COMPARATOR = new Comparator<SuperType>()
        {
            @Override
            public int compare( SuperType left, SuperType right )
            {
                return left.typeId - right.typeId;
            }
        };
    }

    public static final class Limit<T>
    {
        public final T value;
        public final boolean isInclusive;

        private Limit( T value, boolean isInclusive )
        {
            this.value = value;
            this.isInclusive = isInclusive;
        }

        public <X extends T> X castValue( Class<X> clazz )
        {
            return clazz.cast( value );
        }

        public static <T> Limit<T> inclusive( T value )
        {
            return new Limit<>( value, true );
        }

        public static <T> Limit<T> exclusive( T value )
        {
            return new Limit<>( value, false );
        }
    }

    private static class AnyPropertyValueComparator extends PropertyValueComparator<Object>
    {
        @Override
        public int compare( Object left, Object right )
        {
            SuperType leftType = SuperType.ofValue( left );
            SuperType rightType = SuperType.ofValue( right );
            int cmp = COMPARE_SUPER_TYPE.compare( leftType, rightType );

            if ( cmp == 0 )
            {
                switch ( leftType )
                {
                    case NUMBER:
                        return COMPARE_NUMBERS.compare( (Number) left, (Number) right );

                    case STRING:
                        return left.toString().compareTo( right.toString() );

                    case BOOLEAN:
                        return Boolean.compare( (Boolean) left, (Boolean) right );

                    // case OTHER:
                    default:
                        String leftString = ObjectUtil.toString( left );
                        String rightString = ObjectUtil.toString( right );
                        return leftString.compareTo( rightString );
                }
            }
            else
            {
                return cmp;
            }
        }
    }

    private static class PropertyValueSuperTypeComparator extends PropertyValueComparator<SuperType>
    {
        @Override
        public int compare( SuperType left, SuperType right )
        {
            return Integer.compare( left.typeId, right.typeId );
        }
    }

    private static class NumberPropertyValueComparator extends PropertyValueComparator<Number>
    {
        @SuppressWarnings("unchecked")
        @Override
        public int compare( Number left, Number right )
        {
            Class<? extends Number> leftClazz = left.getClass();
            Class<? extends Number> rightClazz = right.getClass();
            if ( leftClazz == rightClazz )
            {
                return ((Comparable<Number>) left).compareTo( right );
            }
            else
            {
                if ( (left instanceof Double) || (left instanceof Float) )
                {
                    double lhs = left.doubleValue();
                    if ( right instanceof Long )
                    {
                        long rhs = right.longValue();
                        return MathUtil.compareDoubleAgainstLong( lhs, rhs );
                    }
                    return Double.compare( lhs, right.doubleValue() );
                }

                if ( (right instanceof Double) || (right instanceof Float) )
                {
                    double rhs = right.doubleValue();
                    if ( left instanceof Long )
                    {
                        long lhs = left.longValue();
                        return MathUtil.compareLongAgainstDouble( lhs, rhs );
                    }
                    return Double.compare( left.doubleValue(), rhs );
                }

                // Compare supported mixed integral types by least upper bound compare

                if ( leftClazz.equals( Long.class ) )
                {
                    if ( rightClazz.equals( Integer.class ) || rightClazz.equals( Short.class ) || rightClazz.equals(
                            Byte.class ) )
                    {
                        return Long.compare( left.longValue(), right.longValue() );
                    }
                }
                else if ( leftClazz.equals( Integer.class ) )
                {
                    if ( rightClazz.equals( Long.class ) )
                    {
                        return Long.compare( left.longValue(), right.longValue() );
                    }
                    else if ( rightClazz.equals( Short.class ) || rightClazz.equals( Byte.class ) )
                    {
                        return Integer.compare( left.intValue(), right.intValue() );
                    }
                }
                else if ( leftClazz.equals( Short.class ) )
                {
                    if ( rightClazz.equals( Long.class ) )
                    {
                        return Long.compare( left.longValue(), right.longValue() );
                    }
                    else if ( rightClazz.equals( Integer.class ) )
                    {
                        return Integer.compare( left.intValue(), right.intValue() );
                    }
                    else if ( rightClazz.equals( Byte.class ) )
                    {
                        return Short.compare( left.shortValue(), right.shortValue() );
                    }
                }
                else if ( leftClazz.equals( Byte.class ) )
                {
                    if ( rightClazz.equals( Long.class ) )
                    {
                        return Long.compare( left.longValue(), right.longValue() );
                    }
                    else if ( rightClazz.equals( Integer.class ) )
                    {
                        return Integer.compare( left.intValue(), right.intValue() );
                    }
                    else if ( rightClazz.equals( Short.class ) )
                    {
                        return Short.compare( left.shortValue(), right.shortValue() );
                    }
                }

                throw new IllegalArgumentException( format(
                        "Comparing numbers %s and %s (with type %s and %s) is not supported",
                        left, right,
                        leftClazz, rightClazz
                ) );
            }
        }
    }

    private static class StringPropertyValueComparator extends PropertyValueComparator<Object>
    {
        @Override
        public int compare( Object left, Object right )
        {
            return convert( left ).compareTo( convert( right ) );
        }

        private String convert( Object value )
        {
            Class<?> clazz = value.getClass();
            if ( clazz.equals( String.class ) || clazz.equals( Character.class ) )
            {
                return value.toString();
            }
            else
            {
                throw new IllegalArgumentException( format( "Cannot compare %s as a string", value ) );
            }
        }
    }
}
