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
package org.neo4j.values.storable;

import java.util.Arrays;

/**
 * Static methods for checking the equality of arrays of primitives.
 *
 * This class handles only evaluation of a[] == b[] where type( a ) != type( b ), ei. byte[] == int[] and such.
 * byte[] == byte[] evaluation can be done using Arrays.equals().
 */
public final class PrimitiveArrayValues
{
    private PrimitiveArrayValues()
    {
    }

    // TYPED COMPARISON

    public static boolean equals( byte[] a, short[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( byte[] a, int[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( byte[] a, long[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( byte[] a, float[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( byte[] a, double[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( short[] a, int[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( short[] a, long[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( short[] a, float[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( short[] a, double[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( int[] a, long[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( int[] a, float[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( int[] a, double[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( long[] a, float[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( long[] a, double[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( float[] a, double[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            if ( a[i] != b[i] )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean equals( char[] a, String[] b )
    {
        if ( a.length != b.length )
        {
            return false;
        }

        for ( int i = 0; i < a.length; i++ )
        {
            String str = b[i];
            if ( str == null || str.length() != 1 || str.charAt( 0 ) != a[i] )
            {
                return false;
            }
        }
        return true;
    }

    // NON-TYPED COMPARISON

    public static boolean equalsObject( byte[] a, Object b )
    {
        if ( b instanceof byte[] )
        {
            return Arrays.equals( a, (byte[]) b );
        }
        else if ( b instanceof short[] )
        {
            return equals( a, (short[]) b );
        }
        else if ( b instanceof int[] )
        {
            return equals( a, (int[]) b );
        }
        else if ( b instanceof long[] )
        {
            return equals( a, (long[]) b );
        }
        else if ( b instanceof float[] )
        {
            return equals( a, (float[]) b );
        }
        else if ( b instanceof double[] )
        {
            return equals( a, (double[]) b );
        }
        return false;
    }

    public static boolean equalsObject( short[] a, Object b )
    {
        if ( b instanceof byte[] )
        {
            return equals( (byte[]) b, a );
        }
        else if ( b instanceof short[] )
        {
            return Arrays.equals( a, (short[]) b );
        }
        else if ( b instanceof int[] )
        {
            return equals( a, (int[]) b );
        }
        else if ( b instanceof long[] )
        {
            return equals( a, (long[]) b );
        }
        else if ( b instanceof float[] )
        {
            return equals( a, (float[]) b );
        }
        else if ( b instanceof double[] )
        {
            return equals( a, (double[]) b );
        }
        return false;
    }

    public static boolean equalsObject( int[] a, Object b )
    {
        if ( b instanceof byte[] )
        {
            return equals( (byte[]) b, a );
        }
        else if ( b instanceof short[] )
        {
            return equals( (short[]) b, a );
        }
        else if ( b instanceof int[] )
        {
            return Arrays.equals( a, (int[]) b );
        }
        else if ( b instanceof long[] )
        {
            return equals( a, (long[]) b );
        }
        else if ( b instanceof float[] )
        {
            return equals( a, (float[]) b );
        }
        else if ( b instanceof double[] )
        {
            return equals( a, (double[]) b );
        }
        return false;
    }

    public static boolean equalsObject( long[] a, Object b )
    {
        if ( b instanceof byte[] )
        {
            return equals( (byte[]) b, a );
        }
        else if ( b instanceof short[] )
        {
            return equals( (short[]) b, a );
        }
        else if ( b instanceof int[] )
        {
            return equals( (int[]) b, a );
        }
        else if ( b instanceof long[] )
        {
            return Arrays.equals( a, (long[]) b );
        }
        else if ( b instanceof float[] )
        {
            return equals( a, (float[]) b );
        }
        else if ( b instanceof double[] )
        {
            return equals( a, (double[]) b );
        }
        return false;
    }

    public static boolean equalsObject( float[] a, Object b )
    {
        if ( b instanceof byte[] )
        {
            return equals( (byte[]) b, a );
        }
        else if ( b instanceof short[] )
        {
            return equals( (short[]) b, a );
        }
        else if ( b instanceof int[] )
        {
            return equals( (int[]) b, a );
        }
        else if ( b instanceof long[] )
        {
            return equals( (long[]) b, a );
        }
        else if ( b instanceof float[] )
        {
            return Arrays.equals( a, (float[]) b );
        }
        else if ( b instanceof double[] )
        {
            return equals( a, (double[]) b );
        }
        return false;
    }

    public static boolean equalsObject( double[] a, Object b )
    {
        if ( b instanceof byte[] )
        {
            return equals( (byte[]) b, a );
        }
        else if ( b instanceof short[] )
        {
            return equals( (short[]) b, a );
        }
        else if ( b instanceof int[] )
        {
            return equals( (int[]) b, a );
        }
        else if ( b instanceof long[] )
        {
            return equals( (long[]) b, a );
        }
        else if ( b instanceof float[] )
        {
            return equals( (float[]) b, a );
        }
        else if ( b instanceof double[] )
        {
            return Arrays.equals( a, (double[]) b );
        }
        return false;
    }

    public static boolean equalsObject( char[] a, Object b )
    {
        if ( b instanceof char[] )
        {
            return Arrays.equals( a, (char[]) b );
        }
        else if ( b instanceof String[] )
        {
            return equals( a, (String[]) b );
        }
        // else if ( other instanceof String ) // should we perhaps support this?
        return false;
    }

    public static boolean equalsObject( String[] a, Object b )
    {
        if ( b instanceof char[] )
        {
            return equals( (char[]) b, a );
        }
        else if ( b instanceof String[] )
        {
            return Arrays.equals( a, (String[]) b );
        }
        // else if ( other instanceof String ) // should we perhaps support this?
        return false;
    }
}
