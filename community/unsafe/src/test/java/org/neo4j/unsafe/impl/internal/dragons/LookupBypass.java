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
package org.neo4j.unsafe.impl.internal.dragons;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Used by the DelegateFileDispatcher, because you cannot access the Lookup object from within certain packages.
 */
public class LookupBypass
{
    private final MethodHandles.Lookup lookup;

    public LookupBypass()
    {
        lookup = MethodHandles.lookup();
    }

    public Class<?> lookupClass()
    {
        return lookup.lookupClass();
    }

    public MethodHandle bind( Object receiver, String name, MethodType type )
            throws NoSuchMethodException, IllegalAccessException
    {
        return lookup.bind( receiver, name, type );
    }

    public MethodHandle unreflectGetter( Field f ) throws IllegalAccessException
    {
        return lookup.unreflectGetter( f );
    }

    public int lookupModes()
    {
        return lookup.lookupModes();
    }

    public MethodHandle findStatic( Class<?> refc, String name, MethodType type )
            throws NoSuchMethodException, IllegalAccessException
    {
        return lookup.findStatic( refc, name, type );
    }

    public MethodHandle findConstructor( Class<?> refc, MethodType type )
            throws NoSuchMethodException, IllegalAccessException
    {
        return lookup.findConstructor( refc, type );
    }

    public MethodHandle findVirtual( Class<?> refc, String name, MethodType type )
            throws NoSuchMethodException, IllegalAccessException
    {
        return lookup.findVirtual( refc, name, type );
    }

    public MethodHandle unreflect( Method m ) throws IllegalAccessException
    {
        return lookup.unreflect( m );
    }

    public MethodHandle findSpecial( Class<?> refc, String name, MethodType type,
                                     Class<?> specialCaller ) throws NoSuchMethodException, IllegalAccessException
    {
        return lookup.findSpecial( refc, name, type, specialCaller );
    }

    public MethodHandle unreflectConstructor( Constructor c ) throws IllegalAccessException
    {
        return lookup.unreflectConstructor( c );
    }

    public MethodHandle findSetter( Class<?> refc, String name, Class<?> type )
            throws NoSuchFieldException, IllegalAccessException
    {
        return lookup.findSetter( refc, name, type );
    }

    public MethodHandle findGetter( Class<?> refc, String name, Class<?> type )
            throws NoSuchFieldException, IllegalAccessException
    {
        return lookup.findGetter( refc, name, type );
    }

    public MethodHandles.Lookup in( Class<?> requestedLookupClass )
    {
        return lookup.in( requestedLookupClass );
    }

    public MethodHandle findStaticGetter( Class<?> refc, String name, Class<?> type )
            throws NoSuchFieldException, IllegalAccessException
    {
        return lookup.findStaticGetter( refc, name, type );
    }

    public MethodHandle findStaticSetter( Class<?> refc, String name, Class<?> type )
            throws NoSuchFieldException, IllegalAccessException
    {
        return lookup.findStaticSetter( refc, name, type );
    }

    public MethodHandle unreflectSpecial( Method m, Class<?> specialCaller ) throws IllegalAccessException
    {
        return lookup.unreflectSpecial( m, specialCaller );
    }

    public MethodHandle unreflectSetter( Field f ) throws IllegalAccessException
    {
        return lookup.unreflectSetter( f );
    }
}
