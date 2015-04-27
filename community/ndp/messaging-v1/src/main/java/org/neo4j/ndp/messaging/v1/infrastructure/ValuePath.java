/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.messaging.v1.infrastructure;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

public class ValuePath implements Path
{
    private final PropertyContainer[] entities;

    public ValuePath( PropertyContainer... entities )
    {
        this.entities = entities;
    }

    @Override
    public Node startNode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node endNode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Relationship lastRelationship()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> reverseRelationships()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Node> nodes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Node> reverseNodes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int length()
    {
        return (entities.length - 1) / 2;
    }

    @Override
    public Iterator<PropertyContainer> iterator()
    {
        return new Iterator<PropertyContainer>()
        {
            private int idx = 0;

            @Override
            public boolean hasNext()
            {
                return idx < entities.length;
            }

            @Override
            public PropertyContainer next()
            {
                return entities[idx++];
            }

            @Override
            public void remove()
            {

            }
        };
    }

    @Override
    public String toString()
    {
        return "ValuePath{" +
               "entities=" + Arrays.toString( entities ) +
               '}';
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

        ValuePath that = (ValuePath) o;

        return Arrays.equals( entities, that.entities );

    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( entities );
    }
}
