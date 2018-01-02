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
package org.neo4j.codegen;

import static org.neo4j.codegen.TypeReference.typeReference;
import static org.neo4j.codegen.TypeReference.typeReferences;

public class MethodReference
{
    public static MethodReference methodReference( Class<?> owner, Class<?> returns, String name,
            Class<?>... parameters )
    {
        return methodReference( typeReference( owner ), typeReference( returns ), name, typeReferences( parameters ) );
    }

    public static MethodReference methodReference( Class<?> owner, TypeReference returns, String name,
            Class<?>... parameters )
    {
        return methodReference( owner, returns, name, typeReferences( parameters ) );
    }

    public static MethodReference methodReference( Class<?> owner, TypeReference returns, String name,
            TypeReference... parameters )
    {
        return methodReference( typeReference( owner ), returns, name, parameters );
    }

    public static MethodReference methodReference( TypeReference owner, TypeReference returns, String name,
            TypeReference... parameters )
    {
        return new MethodReference( owner, name );
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
        return new MethodReference( owner, "<init>" );
    }

    private final TypeReference owner;
    private final String name;

    MethodReference( TypeReference owner, String name )
    {
        this.owner = owner;

        this.name = name;
    }

    public String name()
    {
        return name;
    }

    public TypeReference owner()
    {
        return owner;
    }

    public boolean isConstructor()
    {
        return "<init>".equals( name );
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
