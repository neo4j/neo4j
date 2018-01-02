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
package org.neo4j.graphalgo.impl.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Paths;
import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.helpers.collection.ReverseArrayIterator;

import static org.neo4j.helpers.collection.IteratorUtil.iteratorsEqual;

public final class PathImpl implements Path
{
    public static final class Builder
    {
        private final Builder previous;
        private final Node start;
        private final Relationship relationship;
        private final int size;

        public Builder( Node start )
        {
            if ( start == null )
            {
                throw new NullPointerException();
            }
            this.start = start;
            this.previous = null;
            this.relationship = null;
            this.size = 0;
        }

        private Builder( Builder prev, Relationship rel )
        {
            this.start = prev.start;
            this.previous = prev;
            this.relationship = rel;
            this.size = prev.size + 1;
        }

        public Node getStartNode()
        {
            return start;
        }

        public Path build()
        {
            return new PathImpl( this, null );
        }

        public Builder push( Relationship relationship )
        {
            if ( relationship == null )
            {
                throw new NullPointerException();
            }
            return new Builder( this, relationship );
        }

        public Path build( Builder other )
        {
            return new PathImpl( this, other );
        }

        @Override
        public String toString()
        {
            if ( previous == null )
            {
                return start.toString();
            }
            else
            {
                return relToString( relationship ) + ":" + previous.toString();
            }
        }
    }

    private static String relToString( Relationship rel )
    {
        return rel.getStartNode() + "--" + rel.getType() + "-->"
               + rel.getEndNode();
    }

    private final Node start;
    private final Relationship[] path;
    private final Node end;

    private PathImpl( Builder left, Builder right )
    {
        Node endNode = null;
        path = new Relationship[left.size + ( right == null ? 0 : right.size )];
        if ( right != null )
        {
            for ( int i = left.size, total = i + right.size; i < total; i++ )
            {
                path[i] = right.relationship;
                right = right.previous;
            }
            assert right.relationship == null : "right Path.Builder size error";
            endNode = right.start;
        }

        for ( int i = left.size - 1; i >= 0; i-- )
        {
            path[i] = left.relationship;
            left = left.previous;
        }
        assert left.relationship == null : "left Path.Builder size error";
        start = left.start;
        end = endNode;
    }

    public static Path singular( Node start )
    {
        return new Builder( start ).build();
    }

    public Node startNode()
    {
        return start;
    }

    public Node endNode()
    {
        if ( end != null )
        {
            return end;
        }

        // TODO We could really figure this out in the constructor
        Node stepNode = null;
        for ( Node node : nodes() )
        {
            stepNode = node;
        }
        return stepNode;
    }

    public Relationship lastRelationship()
    {
        return path != null && path.length > 0 ? path[path.length - 1] : null;
    }

    public Iterable<Node> nodes()
    {
        return nodeIterator( start, relationships() );
    }
    
    @Override
    public Iterable<Node> reverseNodes()
    {
        return nodeIterator( endNode(), reverseRelationships() );
    }

    private Iterable<Node> nodeIterator( final Node start, final Iterable<Relationship> relationships )
    {
        return new Iterable<Node>()
        {
            public Iterator<Node> iterator()
            {
                return new Iterator<Node>()
                {
                    Node current = start;
                    int index = 0;
                    Iterator<Relationship> relationshipIterator = relationships.iterator();

                    public boolean hasNext()
                    {
                        return index <= path.length;
                    }

                    public Node next()
                    {
                        if ( current == null )
                        {
                            throw new NoSuchElementException();
                        }
                        Node next = null;
                        if ( index < path.length )
                        {
                            if ( !relationshipIterator.hasNext() )
                            {
                                throw new IllegalStateException( String.format( "Number of relationships: %d does not" +
                                                      " match with path length: %d.", index, path.length ) );
                            }
                            next = relationshipIterator.next().getOtherNode( current );
                        }
                        index += 1;
                        try
                        {
                            return current;
                        }
                        finally
                        {
                            current = next;
                        }
                    }

                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public Iterable<Relationship> relationships()
    {
        return new Iterable<Relationship>()
        {
            @Override
            public Iterator<Relationship> iterator()
            {
                return new ArrayIterator<>( path );
            }
        };
    }
    
    @Override
    public Iterable<Relationship> reverseRelationships()
    {
        return new Iterable<Relationship>()
        {
            @Override
            public Iterator<Relationship> iterator()
            {
                return new ReverseArrayIterator<>( path );
            }
        };
    }

    public Iterator<PropertyContainer> iterator()
    {
        return new Iterator<PropertyContainer>()
        {
            Iterator<? extends PropertyContainer> current = nodes().iterator();
            Iterator<? extends PropertyContainer> next = relationships().iterator();

            public boolean hasNext()
            {
                return current.hasNext();
            }

            public PropertyContainer next()
            {
                try
                {
                    return current.next();
                }
                finally
                {
                    Iterator<? extends PropertyContainer> temp = current;
                    current = next;
                    next = temp;
                }
            }

            public void remove()
            {
                next.remove();
            }
        };
    }

    public int length()
    {
        return path.length;
    }

    @Override
    public int hashCode()
    {
        if ( path.length == 0 )
        {
            return start.hashCode();
        }
        else
        {
            return Arrays.hashCode( path );
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        else if ( obj instanceof Path )
        {
            Path other = (Path) obj;
            if ( !start.equals( other.startNode() ) )
            {
                return false;
            }

            return iteratorsEqual( this.relationships().iterator(), other.relationships().iterator() );
        }
        else
        {
            return false;
        }
    }

    @Override
    public String toString()
    {
        return Paths.defaultPathToString( this );
    }
}
