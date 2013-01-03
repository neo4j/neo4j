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
package org.neo4j.cypher.pycompat;

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

public class WrappedPath implements Path
{
    private final Path inner;

    public WrappedPath( Path inner )
    {
        this.inner = inner;
    }

    @Override
    public Node startNode()
    {
        return inner.startNode();
    }

    @Override
    public Node endNode()
    {
        return inner.endNode();
    }

    @Override
    public Relationship lastRelationship()
    {
        return inner.lastRelationship();
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        return new WrappedIterable<Relationship>(inner.relationships());
    }

    @Override
    public Iterable<Relationship> reverseRelationships()
    {
        return new WrappedIterable<Relationship>(inner.reverseRelationships());
    }

    @Override
    public Iterable<Node> nodes()
    {
        return new WrappedIterable<Node>(inner.nodes());
    }

    @Override
    public Iterable<Node> reverseNodes()
    {
        return new WrappedIterable<Node>(inner.reverseNodes());
    }

    @Override
    public int length()
    {
        return inner.length();
    }

    @Override
    public Iterator<PropertyContainer> iterator()
    {
        return new WrappedIterator<PropertyContainer>(inner.iterator());
    }
}
