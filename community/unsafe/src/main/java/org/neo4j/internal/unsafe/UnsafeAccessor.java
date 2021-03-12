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
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

final class UnsafeAccessor
{
    private UnsafeAccessor()
    {
    }

    static Unsafe getUnsafe()
    {
        try
        {
            PrivilegedExceptionAction<Unsafe> getUnsafe = () ->
            {
                try
                {
                    return Unsafe.getUnsafe();
                }
                catch ( Exception e )
                {
                    Class<Unsafe> type = Unsafe.class;
                    Field[] fields = type.getDeclaredFields();
                    for ( Field field : fields )
                    {
                        if ( Modifier.isStatic( field.getModifiers() )
                                && type.isAssignableFrom( field.getType() ) )
                        {
                            field.setAccessible( true );
                            return type.cast( field.get( null ) );
                        }
                    }
                    LinkageError error = new LinkageError( "No static field of type sun.misc.Unsafe" );
                    error.addSuppressed( e );
                    throw error;
                }
            };
            return AccessController.doPrivileged( getUnsafe );
        }
        catch ( Exception e )
        {
            throw new LinkageError( "Cannot access sun.misc.Unsafe", e );
        }
    }
}
