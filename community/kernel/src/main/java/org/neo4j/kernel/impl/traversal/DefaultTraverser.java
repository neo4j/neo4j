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
package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;

public class DefaultTraverser implements Traverser
{
    private final Factory<TraverserIterator> traverserIteratorFactory;

    private TraversalMetadata lastIterator;

    DefaultTraverser( Factory<TraverserIterator> traverserIteratorFactory )
    {
        this.traverserIteratorFactory = traverserIteratorFactory;
    }

    @Override
    public Iterable<Node> nodes()
    {
        return new PathIterableWrapper<>( this )
        {
            @Override
            protected Node convert( Path path )
            {
                return path.endNode();
            }
        };
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        return new PathIterableWrapper<>( this )
        {
            @Override
            public Iterator<Relationship> iterator()
            {
                var pathIterator = pathIterator();
                return new PrefetchingIterator<>()
                {
                    @Override
                    protected Relationship fetchNextOrNull()
                    {
                        while ( pathIterator.hasNext() )
                        {
                            Path path = pathIterator.next();
                            if ( path.length() > 0 )
                            {
                                return path.lastRelationship();
                            }
                        }
                        return null;
                    }
                };
            }

            @Override
            protected Relationship convert( Path path )
            {
                return path.lastRelationship();
            }
        };
    }

    @Override
    public Iterator<Path> iterator()
    {
        TraverserIterator traverserIterator = traverserIteratorFactory.newInstance();
        lastIterator = traverserIterator;
        return traverserIterator;
    }

    @Override
    public TraversalMetadata metadata()
    {
        return lastIterator;
    }

    private abstract static class PathIterableWrapper<T> implements Iterable<T>
    {
        private final Iterable<Path> iterableToWrap;

        PathIterableWrapper( Iterable<Path> iterableToWrap )
        {
            this.iterableToWrap = iterableToWrap;
        }

        Iterator<Path> pathIterator()
        {
            return iterableToWrap.iterator();
        }

        @Override
        public Iterator<T> iterator()
        {
            var iterator = pathIterator();
            return new PrefetchingIterator<>()
            {
                @Override
                protected T fetchNextOrNull()
                {
                    return iterator.hasNext() ? convert( iterator.next() ) : null;
                }
            };
        }

        protected abstract T convert( Path path );
    }
}
