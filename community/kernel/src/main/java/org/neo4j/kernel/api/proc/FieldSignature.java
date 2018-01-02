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
package org.neo4j.kernel.api.proc;

import java.util.Objects;
import java.util.Optional;

import org.neo4j.kernel.impl.proc.Neo4jValue;

import static java.util.Objects.requireNonNull;

/** Represents a type and a name for a field in a record, used to define input and output record signatures. */
public class FieldSignature
{
    public static FieldSignature inputField( String name, Neo4jTypes.AnyType type )
    {
        return new FieldSignature( name, type, null, false );
    }

    public static FieldSignature inputField( String name, Neo4jTypes.AnyType type, Neo4jValue defaultValue )
    {
        return new FieldSignature( name, type, requireNonNull( defaultValue, "defaultValue" ), false );
    }

    public static FieldSignature outputField( String name, Neo4jTypes.AnyType type )
    {
        return outputField( name, type, false );
    }

    public static FieldSignature outputField( String name, Neo4jTypes.AnyType type, boolean deprecated )
    {
        return new FieldSignature( name, type, null, deprecated );
    }

    private final String name;
    private final Neo4jTypes.AnyType type;
    private final Neo4jValue defaultValue;
    private final boolean deprecated;

    private FieldSignature( String name, Neo4jTypes.AnyType type, Neo4jValue defaultValue, boolean deprecated )
    {
        this.name = requireNonNull( name, "name" );
        this.type = requireNonNull( type, "type" );
        this.defaultValue = defaultValue;
        this.deprecated = deprecated;
        if ( defaultValue != null )
        {
            if ( !type.equals( defaultValue.neo4jType() ) )
            {
                throw new IllegalArgumentException( String.format(
                        "Default value does not have a valid type, field type was %s, but value type was %s.",
                        type.toString(), defaultValue.neo4jType().toString() ) );
            }
        }
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
        return Optional.ofNullable( defaultValue );
    }

    public boolean isDeprecated()
    {
        return deprecated;
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        result.append( name );
        if ( defaultValue != null )
        {
            result.append( " = " ).append( defaultValue.value() );
        }
        return result.append( " :: " ).append( type ).toString();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        FieldSignature that = (FieldSignature) o;
        return name.equals( that.name ) &&
                type.equals( that.type ) &&
                Objects.equals( this.defaultValue, that.defaultValue ) &&
                this.deprecated == that.deprecated;
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
