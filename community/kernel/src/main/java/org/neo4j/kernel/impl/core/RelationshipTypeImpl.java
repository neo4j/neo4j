/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.RelationshipType;

class RelationshipTypeImpl implements RelationshipType
{
    final String name;
    private final int id;

    RelationshipTypeImpl( String name, int id )
    {
        assert name != null;
        this.name = name;
        this.id = id;
    }

    public String name()
    {
        return name;
    }
    
    public int getId()
    {
        return id;
    }

    public String toString()
    {
        return name;
    }

    public boolean equals( Object o )
    {
        if ( !(o instanceof RelationshipType) )
        {
            return false;
        }
        return name.equals( ((RelationshipType) o).name() );
    }

    public int hashCode()
    {
        return name.hashCode();
    }
}
