/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.helpers.ObjectUtil;

import static java.lang.String.format;

public class PropertyValue
{
    // This compares two values that have the same super type according to that super type's comparator
    // Any values that fall under OTHER, are compared by ObjectUtil.toString
    // NULL is considered the smallest value
    public final static Comparator<Object> COMPARE_WITH_LEFT_NULLS = new AnyValuesComparator()
    {
        @Override
        public int compare( Object left, Object right )
        {
            if ( left == right )
            {
                return 0;
            }
            else if ( left == null )
            {
                return -1;
            }
            else if ( right == null )
            {
                return +1;
            }

            return super.compare( left, right );
        }
    };

    // This compares two values that have the same super type according to that super type's comparator
    // Any values that fall under OTHER, are compared by ObjectUtil.toString
    // NULL is considered the largest value
    public final static Comparator<Object> COMPARE_WITH_RIGHT_NULLS = new AnyValuesComparator()
    {
        @Override
        public int compare( Object left, Object right )
        {
            if ( left == right )
            {
                return 0;
            }
            else if ( left == null )
            {
                return +1;
            }
            else if ( right == null )
            {
                return -1;
            }

            return super.compare( left, right );
        }
    };

    // This compares two values that have the same super type according to that super type's comparator
    // If the shared super type is OTHER, an exception is thrown
    // NULL is considered the smallest value
    public final static Comparator<Object> COMPARE_OR_FAIL_WITH_LEFT_NULLS = new TypedValuesComparator()
    {
        @Override
        public int compare( Object left, Object right )
        {
            if ( left == right )
            {
                return 0;
            }
            else if ( left == null )
            {
                return -1;
            }
            else if ( right == null )
            {
                return +1;
            }

            return super.compare( left, right );
        }

        @Override
        protected int compareOthers( SuperType leftType, Object left, SuperType rightType, Object right )
        {
            throw new UnsupportedOperationException( format(
                    "Cannot compare %s %s with %s %s",
                    leftType, left,
                    rightType, right
            ) );
        }
    };

    public final static Comparator<Number> NUMBER_COMPARATOR = new Comparator<Number>()
    {
        @SuppressWarnings("unchecked")
        @Override
        public int compare( Number left, Number right )
        {
            if ( left.getClass() == right.getClass() )
            {
                return ((Comparable<Number>) left).compareTo( right );
            }
            else
            {
                if ( (left instanceof Double) || (right instanceof Double) )
                {
                    return Double.compare( left.doubleValue(), right.doubleValue() );
                }
                else if ( (left instanceof Float) || (right instanceof Float) )
                {
                    return Float.compare( left.floatValue(), right.floatValue() );
                }
                else
                {
                    return Long.compare( left.longValue(), right.longValue() );
                }
            }
        }
    };

    private static class AnyValuesComparator extends TypedValuesComparator
    {
        @Override
        protected int compareOthers( SuperType leftType, Object left, SuperType rightType, Object right )
        {
            int cmp = SuperType.TYPE_ID_COMPARATOR.compare( leftType, rightType );
            if ( cmp == 0 )
            {
                return ObjectUtil.toString( left ).compareTo( ObjectUtil.toString( right ) );
            }
            else
            {
                return cmp;
            }
        }
    }
    private abstract static class TypedValuesComparator implements Comparator<Object>
    {
        @Override
        public int compare( Object left, Object right )
        {
            SuperType leftType = SuperType.ofValue( left );
            SuperType rightType = SuperType.ofValue( right );
            SuperType type = SuperType.ofTypes( leftType, rightType );

            switch ( type )
            {
                case NUMBER:
                    return NUMBER_COMPARATOR.compare( (Number) left, (Number) right );

                case STRING:
                    return left.toString().compareTo( right.toString() );

                case BOOLEAN:
                    return Boolean.compare( (Boolean) left, (Boolean) right );

                // case OTHER:
                default: return compareOthers( leftType, left, rightType, right );
            }
        }

        protected abstract int compareOthers( SuperType leftType, Object left, SuperType rightType, Object right );
    }

    public enum SuperType
    {
        NUMBER( 0 ),
        STRING( 1 ),
        BOOLEAN( 2 ),
        OTHER( 255 );

        public final int typeId;

        SuperType( int id )
        {
            typeId = id;
        }

        public boolean isSuperTypeOf( Object value )
        {
            return this == ofValue( value );
        }

        public static SuperType ofTypes( SuperType leftType, SuperType rightType )
        {

            return leftType == rightType ? leftType : OTHER;
        }

        public static SuperType ofValue( Object value )
        {
            if ( value instanceof String )
            {
                return STRING;
            } else if ( value instanceof Character )
            {
                return STRING;
            } else if ( value instanceof Number )
            {
                return NUMBER;
            } else if ( value instanceof Boolean )
            {
                return BOOLEAN;
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
}
