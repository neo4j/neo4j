/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.values.storable.helpers;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

import org.neo4j.function.ThrowingSupplier;

public class UnsafeStringUtils
{
    private static final Unsafe unsafe;
    /**
     * Offset to the underlying char[] value in java.lang.String
     */
    private static final long valueOffset;

    /**
     * Some jvms also use an offset marking the start of the string in the char[]
     */
    private static final long offsetOffset;
    private static final long NOT_FOUND = -1L;

    static
    {
        unsafe = theUnsafe();
        valueOffset = offsetOfDeclaredField( () -> String.class.getDeclaredField( "value" ) );
        offsetOffset = offsetOfDeclaredField( () -> String.class.getDeclaredField( "offset" ) );
    }

    /**
     * Provide backdoor to unsafe
     */
    private static Unsafe theUnsafe()
    {
        Field declaredField;
        try
        {
            declaredField = Unsafe.class.getDeclaredField( "theUnsafe" );
            declaredField.setAccessible( true );
        }
        catch ( Exception e )
        {
            declaredField = null;
        }

        if ( declaredField != null )
        {
            try
            {
                return (Unsafe) declaredField.get( null );
            }
            catch ( IllegalAccessException e )
            {
                return null;
            }
        }
        else
        {
            return null;
        }
    }

    /**
     * Returns the offset of a declared field or {@value NOT_FOUND} if it couldn't be found.
     */
    private static long offsetOfDeclaredField( ThrowingSupplier<Field,Exception> supplier )
    {
        Field declaredField;
        try
        {
            declaredField = supplier.get();
        }
        catch ( Exception e )
        {
            declaredField = null;
        }

        if ( declaredField != null && unsafe != null )
        {
            return unsafe.objectFieldOffset( declaredField );
        }
        else
        {
            return NOT_FOUND;
        }
    }

    public static char[] toCharArray( String s )
    {
        if ( valueOffset == NOT_FOUND )
        {
            //we didn't get to access the internals, we'll stick with a defensive copy
            return s.toCharArray();
        }
        else
        {
            return (char[]) unsafe.getObject( s, valueOffset );
        }
    }

    public static int offsetOf( String s )
    {
        if ( offsetOffset == NOT_FOUND )
        {
            return 0;
        }
        else
        {
            return unsafe.getInt( s, offsetOffset );
        }
    }
}
