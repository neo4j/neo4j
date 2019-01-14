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
package org.neo4j.graphdb.impl;

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.PrefetchingIterator;

public class ExtendedPath implements Path
{
    private final Path start;
    private final Relationship lastRelationship;
    private final Node endNode;

    public ExtendedPath( Path start, Relationship lastRelationship )
    {
        this.start = start;
        this.lastRelationship = lastRelationship;
        this.endNode = lastRelationship.getOtherNode( start.endNode() );
    }

    @Override
    public Node startNode()
    {
        return start.startNode();
    }

    @Override
    public Node endNode()
    {
        return endNode;
    }

    @Override
    public Relationship lastRelationship()
    {
        return lastRelationship;
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        return () -> new PrefetchingIterator<Relationship>()
        {
            final Iterator<Relationship> startRelationships = start.relationships().iterator();
            boolean lastReturned;

            @Override
            protected Relationship fetchNextOrNull()
            {
                if ( startRelationships.hasNext() )
                {
                    return startRelationships.next();
                }
                if ( !lastReturned )
                {
                    lastReturned = true;
                    return lastRelationship;
                }
                return null;
            }
        };
    }

    @Override
    public Iterable<Relationship> reverseRelationships()
    {
        return () -> new PrefetchingIterator<Relationship>()
        {
            final Iterator<Relationship> startRelationships = start.reverseRelationships().iterator();
            boolean endReturned;

            @Override
            protected Relationship fetchNextOrNull()
            {
                if ( !endReturned )
                {
                    endReturned = true;
                    return lastRelationship;
                }
                return startRelationships.hasNext() ? startRelationships.next() : null;
            }
        };
    }

    @Override
    public Iterable<Node> nodes()
    {
        return () -> new PrefetchingIterator<Node>()
        {
            final Iterator<Node> startNodes = start.nodes().iterator();
            boolean lastReturned;

            @Override
            protected Node fetchNextOrNull()
            {
                if ( startNodes.hasNext() )
                {
                    return startNodes.next();
                }
                if ( !lastReturned )
                {
                    lastReturned = true;
                    return endNode;
                }
                return null;
            }
        };
    }

    @Override
    public Iterable<Node> reverseNodes()
    {
        return () -> new PrefetchingIterator<Node>()
        {
            final Iterator<Node> startNodes = start.reverseNodes().iterator();
            boolean endReturned;

            @Override
            protected Node fetchNextOrNull()
            {
                if ( !endReturned )
                {
                    endReturned = true;
                    return endNode;
                }
                return startNodes.hasNext() ? startNodes.next() : null;
            }
        };
    }

    @Override
    public int length()
    {
        return start.length() + 1;
    }

    @Override
    public Iterator<PropertyContainer> iterator()
    {
        return new PrefetchingIterator<PropertyContainer>()
        {
            final Iterator<PropertyContainer> startEntities = start.iterator();
            int lastReturned = 2;

            @Override
            protected PropertyContainer fetchNextOrNull()
            {
                if ( startEntities.hasNext() )
                {
                    return startEntities.next();
                }
                switch ( lastReturned-- )
                {
                case 2: return endNode;
                case 1: return lastRelationship;
                default: return null;
                }
            }
        };
    }

    /**
     * Appends a {@link Relationship relationship}, {@code withRelationship}, to the specified {@link Path path}
     * @param path
     * @param withRelationship
     * @return The path with the relationship and its end node appended.
     */
    public static Path extend( Path path, Relationship withRelationship )
    {
        return new ExtendedPath( path, withRelationship );
    }
}
