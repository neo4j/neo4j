/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.properties;

import java.util.Arrays;
import java.util.concurrent.Callable;

public class LazyArrayProperty extends LazyProperty<Object>
{

    private Type type;

    LazyArrayProperty( long propertyKeyId, final Callable<Object> producer )
    {
        super( propertyKeyId, producer );
    }

    @Override
    protected Object produceValue()
    {
        Object value = super.produceValue();
        this.type = Type.from( value );
        return value;
    }

    @Override
    public boolean valueEquals( Object value )
    {
        Object myValue = value();
        return type.equals( myValue, value );
    }

    @Override
    int valueHash()
    {
        Object myValue = value();
        return type.hashCode( myValue );
    }

    enum Type
    {
        INT
                {
                    int hashCode( Object array )
                    {
                        return Arrays.hashCode( (int[]) array );
                    }

                    boolean equals( Object array1, Object array2 )
                    {
                        return Arrays.equals( (int[]) array1, (int[]) array2 );
                    }
                },
        LONG
                {
                    int hashCode( Object array )
                    {
                        return Arrays.hashCode( (long[]) array );
                    }

                    boolean equals( Object array1, Object array2 )
                    {
                        return Arrays.equals( (long[]) array1, (long[]) array2 );
                    }
                },
        BOOLEAN
                {
                    int hashCode( Object array )
                    {
                        return Arrays.hashCode( (boolean[]) array );
                    }

                    boolean equals( Object array1, Object array2 )
                    {
                        return Arrays.equals( (boolean[]) array1, (boolean[]) array2 );
                    }
                },
        BYTE
                {
                    int hashCode( Object array )
                    {
                        return Arrays.hashCode( (byte[]) array );
                    }

                    boolean equals( Object array1, Object array2 )
                    {
                        return Arrays.equals( (byte[]) array1, (byte[]) array2 );
                    }
                },
        DOUBLE
                {
                    int hashCode( Object array )
                    {
                        return Arrays.hashCode( (double[]) array );
                    }

                    boolean equals( Object array1, Object array2 )
                    {
                        return Arrays.equals( (double[]) array1, (double[]) array2 );
                    }
                },
        STRING
                {
                    int hashCode( Object array )
                    {
                        return Arrays.hashCode( (String[])
                                array );
                    }

                    boolean equals( Object array1, Object array2 )
                    {
                        return Arrays.equals( (String[]) array1, (String[]) array2 );
                    }
                },
        SHORT
                {
                    int hashCode( Object array )
                    {
                        return Arrays.hashCode( (short[]) array );
                    }

                    boolean equals( Object array1, Object array2 )
                    {
                        return Arrays.equals( (short[]) array1, (short[]) array2 );
                    }
                },
        CHAR
                {
                    int hashCode( Object array )
                    {
                        return Arrays.hashCode( (char[]) array );
                    }

                    boolean equals( Object array1, Object array2 )
                    {
                        return Arrays.equals( (char[]) array1, (char[]) array2 );
                    }
                },
        FLOAT
                {
                    int hashCode( Object array )
                    {
                        return Arrays.hashCode( (float[]) array );
                    }

                    boolean equals( Object array1, Object array2 )
                    {
                        return Arrays.equals( (float[]) array1, (float[]) array2 );
                    }
                };

        abstract int hashCode( Object array );

        abstract boolean equals( Object array1, Object array2 );

        public static Type from( Object array )
        {
            if ( array instanceof int[] )
            {
                return INT;
            }
            if ( array instanceof long[] )
            {
                return LONG;
            }
            if ( array instanceof boolean[] )
            {
                return BOOLEAN;
            }
            if ( array instanceof byte[] )
            {
                return BYTE;
            }
            if ( array instanceof double[] )
            {
                return DOUBLE;
            }
            if ( array instanceof String[] )
            {
                return STRING;
            }
            if ( array instanceof short[] )
            {
                return SHORT;
            }
            if ( array instanceof char[] )
            {
                return CHAR;
            }
            if ( array instanceof float[] )
            {
                return FLOAT;
            }
            throw new IllegalStateException( "Not a recognized array type " + array.getClass() );
        }
    }
}
