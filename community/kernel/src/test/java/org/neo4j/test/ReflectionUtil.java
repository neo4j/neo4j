/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import sun.reflect.FieldAccessor;
import sun.reflect.ReflectionFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

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
}
