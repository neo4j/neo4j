/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.unsafe;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static org.neo4j.util.FeatureToggles.flag;

public final class IllegalAccessLoggerSuppressor
{
    private static final boolean PRINT_REFLECTION_EXCEPTIONS = flag( UnsafeUtil.class, "printReflectionExceptions", false );

    private IllegalAccessLoggerSuppressor()
    {
    }

    public static void suppress()
    {
        try
        {
            Unsafe unsafe = UnsafeAccessor.getUnsafe();
            Class<?> clazz = Class.forName( "jdk.internal.module.IllegalAccessLogger" );
            Field logger = clazz.getDeclaredField( "logger" );
            unsafe.putObjectVolatile( clazz, unsafe.staticFieldOffset( logger ), null );
        }
        catch ( Throwable t )
        {
            if ( PRINT_REFLECTION_EXCEPTIONS )
            {
                //noinspection CallToPrintStackTrace
                t.printStackTrace();
            }
        }
    }

}
