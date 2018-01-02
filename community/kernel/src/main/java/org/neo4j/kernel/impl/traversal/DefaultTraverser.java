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
package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.Factory;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;

public class DefaultTraverser implements Traverser
{
    private final Factory<TraverserIterator> traverserIteratorFactory;

    private TraversalMetadata lastIterator;

    DefaultTraverser(Factory<TraverserIterator> traverserIteratorFactory )
    {
        this.traverserIteratorFactory = traverserIteratorFactory;
    }

    @Override
    public ResourceIterable<Node> nodes()
    {
        return new ResourcePathIterableWrapper<Node>( this )
        {
            @Override
            protected Node convert( Path path )
            {
                return path.endNode();
            }
        };
    }

    @Override
    public ResourceIterable<Relationship> relationships()
    {
        return new ResourcePathIterableWrapper<Relationship>( this )
        {
            @Override
            public ResourceIterator<Relationship> iterator()
            {
                final ResourceIterator<Path> pathIterator = pathIterator();
                return new PrefetchingResourceIterator<Relationship>()
                {
                    @Override
                    public void close()
                    {
                        pathIterator.close();
                    }

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
    public ResourceIterator<Path> iterator()
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

    private static abstract class ResourcePathIterableWrapper<T> implements ResourceIterable<T>
    {
        private final ResourceIterable<Path> iterableToWrap;

        protected ResourcePathIterableWrapper( ResourceIterable<Path> iterableToWrap )
        {
            this.iterableToWrap = iterableToWrap;
        }

        protected ResourceIterator<Path> pathIterator()
        {
            return iterableToWrap.iterator();
        }

        @Override
        public ResourceIterator<T> iterator()
        {
            final ResourceIterator<Path> iterator = pathIterator();
            return new PrefetchingResourceIterator<T>()
            {
                @Override
                public void close()
                {
                    iterator.close();
                }

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
