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
package org.neo4j.server.rest.domain;

public class GraphDatabaseName
{
    public static final GraphDatabaseName NO_NAME = new GraphDatabaseName(
            "org.neo4j.rest.domain.GraphDatabaseName.NO_NAME" );
    private final String name;

    public GraphDatabaseName( String name )
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public String toString()
    {
        return getName();
    }

    @Override
    public int hashCode()
    {
        return name != null ? name.hashCode() : 0;
    }

    public boolean equals( Object obj )
    {
        return obj instanceof GraphDatabaseName && ( (GraphDatabaseName) obj ).getName()
                .equals( name );
    }
}
