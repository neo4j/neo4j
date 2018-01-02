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

import java.lang.reflect.Modifier;

public class FieldReference
{
    public static FieldReference field( TypeReference owner, TypeReference type, String name )
    {
        return new FieldReference( Modifier.PUBLIC, owner, type, name );
    }

    public static FieldReference staticField( TypeReference owner, TypeReference type, String name )
    {
        return new FieldReference( Modifier.STATIC | Modifier.PRIVATE, owner, type, name );
    }

    private final int modifiers;
    private final TypeReference owner;
    private final TypeReference type;
    private final String name;

    FieldReference( int modifiers, TypeReference owner, TypeReference type, String name )
    {
        this.modifiers = modifiers;
        this.owner = owner;
        this.type = type;
        this.name = name;
    }

    public TypeReference owner()
    {
        return owner;
    }

    public TypeReference type()
    {
        return type;
    }

    public String name()
    {
        return name;
    }

    public boolean isStatic()
    {
        return Modifier.isStatic( modifiers );
    }

    public boolean isFinal()
    {
        return Modifier.isFinal( modifiers );
    }

    public int modifiers()
    {
        return modifiers;
    }
}
