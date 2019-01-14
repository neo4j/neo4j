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
package org.neo4j.test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ReflectionUtil
{
    private ReflectionUtil()
    {
    }

    public static <T> T getPrivateField( Object target, String fieldName, Class<T> fieldType ) throws Exception
    {
        Class<?> type = target.getClass();
        Field field = getField( fieldName, type );
        if ( !fieldType.isAssignableFrom( field.getType() ) )
        {
            throw new IllegalArgumentException( "Field type does not match " + field.getType() + " is no subclass of " +
                                                "" + fieldType );
        }
        field.setAccessible( true );
        return fieldType.cast( field.get( target ) );
    }

    public static String verifyMethodExists( Class<?> owner, String methodName )
    {
        for ( Method method : owner.getDeclaredMethods() )
        {
            if ( method.getName().equals( methodName ) )
            {
                return methodName;
            }
        }
        throw new IllegalArgumentException( "Method '" + methodName + "' does not exist in class " + owner );
    }

    public static <T> void replaceValueInPrivateField( Object target, String fieldName, Class<T> fieldType, T value )
            throws Exception
    {
        Class<?> type = target.getClass();
        Field field = getField( fieldName, type );
        if ( !fieldType.isAssignableFrom( field.getType() ) )
        {
            throw new IllegalArgumentException( "Field type does not match " + field.getType() + " is no subclass of " +
                    "" + fieldType );
        }
        field.setAccessible( true );
        field.set( target, value );
    }

    private static Field getField( String fieldName, Class<?> type ) throws NoSuchFieldException
    {
        if ( type == null )
        {
            throw new NoSuchFieldException( fieldName );
        }
        try
        {
            Field field = type.getDeclaredField( fieldName );
            if ( field != null )
            {
                return field;
            }
        }
        catch ( NoSuchFieldError | NoSuchFieldException e )
        {
            // Ignore - it might be in the super type
        }
        return getField( fieldName, type.getSuperclass() );
    }
}
