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
package org.neo4j.test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import sun.reflect.FieldAccessor;
import sun.reflect.ReflectionFactory;

public class ReflectionUtil
{
    private static final ReflectionFactory reflectionFactory = ReflectionFactory.getReflectionFactory();

    public static void setStaticFinalField( Field field, Object value )
            throws NoSuchFieldException, IllegalAccessException
    {
        field.setAccessible( true );
        final Field modifiersField = Field.class.getDeclaredField( "modifiers" );
        modifiersField.setAccessible( true );

        int modifiers = modifiersField.getInt( field );
        modifiers &= ~Modifier.FINAL;
        modifiersField.setInt( field, modifiers );

        final FieldAccessor fieldAccessor = reflectionFactory.newFieldAccessor( field, false );
        fieldAccessor.set( null, value );
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

    private static Field getField( String fieldName, Class<? extends Object> type ) throws NoSuchFieldException
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
        catch ( NoSuchFieldError e )
        {
            // Ignore - it might be in the super type
        }
        catch ( NoSuchFieldException e )
        {
            // Ignore - it might be in the super type
        }
        return getField( fieldName, type.getSuperclass() );
    }
}
