/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.proc;

import java.util.Optional;

import org.neo4j.kernel.impl.proc.Neo4jValue;

/** Represents a type and a name for a field in a record, used to define input and output record signatures. */
public class FieldSignature
{
    private final String name;
    private final Neo4jTypes.AnyType type;
    private final Optional<Neo4jValue> defaultValue;

    public FieldSignature( String name, Neo4jTypes.AnyType type)
    {
        this(name, type, Optional.empty());
    }

    public FieldSignature( String name, Neo4jTypes.AnyType type, Optional<Neo4jValue> defaultValue )
    {
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
    }

    public String name()
    {
        return name;
    }

    public Neo4jTypes.AnyType neo4jType()
    {
        return type;
    }

    public Optional<Neo4jValue> defaultValue()
    {
        return defaultValue;
    }

    @Override
    public String toString()
    {
        String nameValue = defaultValue.isPresent() ? name + " = " + defaultValue.get().value() : name;
        return String.format("%s :: %s", nameValue, type);
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) { return true; }
        if ( o == null || getClass() != o.getClass() ) { return false; }
        FieldSignature that = (FieldSignature) o;
        return name.equals( that.name ) && type.equals( that.type );
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
