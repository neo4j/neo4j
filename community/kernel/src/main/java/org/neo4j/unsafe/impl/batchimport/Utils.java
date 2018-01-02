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
package org.neo4j.unsafe.impl.batchimport;

import java.util.NoSuchElementException;

import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.SourceInputIterator;

/**
 * Common and cross-concern utilities.
 */
public class Utils
{
    public static int safeCastLongToInt( long value )
    {
        if ( value > Integer.MAX_VALUE )
        {
            throw new ArithmeticException( getOverflowMessage( value, Integer.TYPE ) );
        }
        return (int) value;
    }

    public static short safeCastLongToShort( long value )
    {
        if ( value > Short.MAX_VALUE )
        {
            throw new ArithmeticException( getOverflowMessage( value, Short.TYPE ) );
        }
        return (short) value;
    }

    public static byte safeCastLongToByte( long value )
    {
        if ( value > Byte.MAX_VALUE )
        {
            throw new ArithmeticException( getOverflowMessage( value, Byte.TYPE ) );
        }
        return (byte) value;
    }

    public enum CompareType
    {
        EQ, GT, GE, LT, LE, NE
    };

    public static boolean unsignedCompare( long dataA, long dataB, CompareType compareType )
    {   // works for signed and unsigned values
        switch ( compareType )
        {
        case EQ:
            return (dataA == dataB);
        case GE:
            if ( dataA == dataB )
            {
                return true;
            }
            // fall through to GT
        case GT:
            return !((dataA < dataB) ^ ((dataA < 0) != (dataB < 0)));
        case LE:
            if ( dataA == dataB )
            {
                return true;
            }
            // fall through to LT
        case LT:
            return ((dataA < dataB) ^ ((dataA < 0) != (dataB < 0)));
        case NE:
        }
        return false;
    }

    /**
     * Like {@link #unsignedCompare(long, long, CompareType)} but reversed in that you get {@link CompareType}
     * from comparing data A and B, i.e. the difference between them.
     */
    public static CompareType unsignedDifference( long dataA, long dataB )
    {
        if ( dataA == dataB )
        {
            return CompareType.EQ;
        }
        return ((dataA < dataB) ^ ((dataA < 0) != (dataB < 0))) ? CompareType.LT : CompareType.GT;
    }

    public static InputIterable<Object> idsOf( final InputIterable<InputNode> nodes )
    {
        return new InputIterable<Object>()
        {
            @Override
            public InputIterator<Object> iterator()
            {
                final InputIterator<InputNode> iterator = nodes.iterator();
                return new SourceInputIterator<Object,InputNode>( iterator )
                {
                    @Override
                    public void close()
                    {
                        iterator.close();
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return iterator.hasNext();
                    }

                    @Override
                    public Object next()
                    {
                        if ( !hasNext() )
                        {
                            throw new NoSuchElementException();
                        }
                        return iterator.next().id();
                    }
                };
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return false;
            }
        };
    }

    // Values in the arrays are assumed to be sorted
    public static boolean anyIdCollides( long[] first, int firstLength, long[] other, int otherLength )
    {
        int f = 0, o = 0;
        while( f < firstLength && o < otherLength )
        {
            if ( first[f] == other[o] )
            {
                return true;
            }

            if ( first[f] < other[o] )
            {
                while ( ++f < firstLength && first[f] < other[o] );
            }
            else
            {
                while ( ++o < otherLength && first[f] > other[o] );
            }
        }

        return false;
    }

    public static void mergeSortedInto( long[] values, long[] into, int intoLengthBefore )
    {
        int v = values.length-1;
        int i = intoLengthBefore-1;
        int t = i+values.length;
        while ( v >= 0 || i >= 0 )
        {
            if ( i == -1 )
            {
                into[t--] = values[v--];
            }
            else if ( v == -1 )
            {
                into[t--] = into[i--];
            }
            else if ( values[v] >= into[i] )
            {
                into[t--] = values[v--];
            }
            else
            {
                into[t--] = into[i--];
            }
        }
    }

    private static String getOverflowMessage( long value, Class clazz )
    {
        return "Value " + value + " is too big to be represented as " + clazz.getName();
    }

    private Utils()
    {   // No instances allowed
    }
}
