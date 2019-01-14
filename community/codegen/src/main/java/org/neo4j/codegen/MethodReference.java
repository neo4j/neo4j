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
package org.neo4j.codegen;

import java.lang.reflect.Modifier;

import static org.neo4j.codegen.TypeReference.typeReference;
import static org.neo4j.codegen.TypeReference.typeReferences;

public class MethodReference
{
    public static MethodReference methodReference( Class<?> owner, Class<?> returns, String name,
            Class<?>... parameters )
    {
        try
        {
            int modifiers = owner.getMethod( name, parameters ).getModifiers();
            return methodReference( typeReference( owner ), typeReference( returns ), name, modifiers, typeReferences( parameters ) );
        }
        catch ( NoSuchMethodException e )
        {
            throw new IllegalArgumentException( "No method with name " + name, e );
        }

    }

    public static MethodReference methodReference( Class<?> owner, TypeReference returns, String name,
            Class<?>... parameters )
    {
        try
        {
            int modifiers = owner.getMethod( name, parameters ).getModifiers();
            return methodReference( owner, returns, name, modifiers, typeReferences( parameters ) );
        }
        catch ( NoSuchMethodException e )
        {
            throw new IllegalArgumentException( "No method with name " + name, e );
        }

    }

    private static MethodReference methodReference( Class<?> owner, TypeReference returns, String name, int modifiers,
            TypeReference... parameters )
    {
        return methodReference( typeReference( owner ), returns, name, modifiers, parameters );
    }

    public static MethodReference methodReference( TypeReference owner, TypeReference returns, String name,
            TypeReference... parameters )
    {
        return new MethodReference( owner, name, returns, Modifier.PUBLIC, parameters );
    }

    public static MethodReference methodReference( TypeReference owner, TypeReference returns, String name,
            int modifiers, TypeReference... parameters )
    {
        return new MethodReference( owner, name, returns, modifiers, parameters );
    }

    public static MethodReference constructorReference( Class<?> owner, Class<?> firstParameter, Class<?>... parameters )
    {
        return constructorReference( typeReference( owner ), typeReferences( firstParameter, parameters ) );
    }

    public static MethodReference constructorReference( Class<?> owner, TypeReference... parameters )
    {
        return constructorReference( typeReference( owner ), parameters );
    }

    public static MethodReference constructorReference( TypeReference owner, TypeReference... parameters )
    {
        return new MethodReference( owner, "<init>", TypeReference.VOID,  Modifier.PUBLIC, parameters );
    }

    private final TypeReference owner;
    private final String name;
    private final TypeReference returns;
    private final TypeReference[] parameters;
    private final int modifiers;

    MethodReference( TypeReference owner, String name, TypeReference returns, int modifiers,
            TypeReference[] parameters )
    {
        this.owner = owner;

        this.name = name;
        this.returns = returns;
        this.modifiers = modifiers;
        this.parameters = parameters;
    }

    public String name()
    {
        return name;
    }

    public TypeReference owner()
    {
        return owner;
    }

    public TypeReference returns()
    {
        return returns;
    }

    public TypeReference[] parameters()
    {
        return parameters;
    }

    public boolean isConstructor()
    {
        return "<init>".equals( name );
    }

    public int modifiers()
    {
        return modifiers;
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder().append( "MethodReference[" );
        writeTo( result );
        return result.append( "]" ).toString();
    }

    void writeTo( StringBuilder result )
    {
        owner.writeTo( result );
        result.append( "#" ).append( name ).append( "(...)" );
    }
}
