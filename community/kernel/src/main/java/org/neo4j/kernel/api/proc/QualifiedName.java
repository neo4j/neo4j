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

import java.util.Arrays;
import java.util.List;

import org.neo4j.helpers.collection.Iterables;

import static java.util.Arrays.asList;

public class QualifiedName
{
    private final String[] namespace;
    private final String name;

    public QualifiedName( List<String> namespace, String name )
    {
        this( namespace.toArray( new String[namespace.size()] ), name );
    }

    public QualifiedName( String[] namespace, String name )
    {
        this.namespace = namespace;
        this.name = name;
    }

    public String[] namespace()
    {
        return namespace;
    }

    public String name()
    {
        return name;
    }

    @Override
    public String toString()
    {
        String strNamespace = namespace.length > 0 ? Iterables.toString( asList( namespace ), "." ) + "." : "";
        return String.format( "%s%s", strNamespace, name );
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

        QualifiedName that = (QualifiedName) o;
        return Arrays.equals( namespace, that.namespace ) && name.equals( that.name );
    }

    @Override
    public int hashCode()
    {
        int result = Arrays.hashCode( namespace );
        result = 31 * result + name.hashCode();
        return result;
    }
}
