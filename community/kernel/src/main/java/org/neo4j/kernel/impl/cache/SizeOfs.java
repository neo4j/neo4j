/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.cache;

import java.lang.reflect.Array;

public class SizeOfs
{
    public static final int REFERENCE_SIZE = 8;

    /**
     * The size of a {@link String} object including object overhead and all state.
     * @param value the String to calculate size for.
     * @return the size of a {@link String} object including object overhead and all state.
     */
    public static int sizeOf( String value )
    {
        return withObjectOverhead( 4/*offset*/ + 4/*count*/ + 4/*hash*/ + REFERENCE_SIZE/*value[] ref*/ +
                withArrayOverhead( +value.length() * 2 )/*value[]*/ );
    }
    
    public static int sizeOfArray( Object value )
    {
        if ( value instanceof String[] )
        {
            int size = 0;
            for ( String string : (String[]) value )
            {
                size += withReference( sizeOf( string ) );
            }
            return withArrayOverhead( size );
        }
        else
        {
            int base;
            if ( value instanceof byte[] || value instanceof boolean[] )
            {
                base = 1;
            }
            else if ( value instanceof short[] || value instanceof char[] )
            {
                base = 2;
            }
            else if ( value instanceof int[] || value instanceof float[] )
            {
                base = 4;
            }
            else if ( value instanceof long[] || value instanceof double[] )
            {
                base = 8;
            }
            else if ( value instanceof Byte[] || value instanceof Boolean[] || 
                    value instanceof Short[] || value instanceof Character[] ||
                    value instanceof Integer[] || value instanceof Float[] ||
                    value instanceof Long[] || value instanceof Double[] )
            {
                // worst case
                base = withObjectOverhead( REFERENCE_SIZE + 8/*value in the boxed Number*/ );
            }
            else
            {
                throw new IllegalStateException( "Unkown type: " + value.getClass() + " [" + value + "]" ); 
            }
            return withArrayOverhead( base * Array.getLength( value ) );
        }
    }
    
    public static int withObjectOverhead( int size )
    {
        // worst case, avg is somewhere between 8-16 depending on heap size
        return 16 + size;
    }
    
    public static int withArrayOverhead( int size )
    {
        // worst case, avg is somewhere between 12-24 depending on heap size
        return 24 + size;
    }
    
    public static int withArrayOverheadIncludingReferences( int size, int length )
    {
        return withArrayOverhead( size + length*REFERENCE_SIZE );
    }
    
    public static int withReference( int size )
    {
        // The standard size of a reference to an object.
        return REFERENCE_SIZE + size;
    }
}
