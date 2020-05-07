/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.commons.lang3.reflect.FieldUtils.readField;

public final class ReflectionUtil
{
    private ReflectionUtil()
    {
    }

    public static <T> T getPrivateField( Object target, String fieldName, Class<T> fieldType ) throws Exception
    {
        return fieldType.cast( readField( target, fieldName, true ) );
    }

    public static void verifyMethodExists( Class<?> owner, String methodName )
    {
        for ( Method method : owner.getDeclaredMethods() )
        {
            if ( methodName.equals( method.getName() ) )
            {
                return;
            }
        }
        throw new IllegalArgumentException( "Method '" + methodName + "' does not exist in class " + owner );
    }

    public static List<Field> getAllFields( Class<?> baseClazz )
    {
        List<Field> fields = new ArrayList<>();
        Class<?> clazz = baseClazz;
        do
        {
            Collections.addAll( fields, clazz.getDeclaredFields() );
            clazz = clazz.getSuperclass();
        }
        while ( clazz != null );
        return fields;
    }
}
