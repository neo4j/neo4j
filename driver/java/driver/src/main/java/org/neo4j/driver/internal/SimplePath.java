/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.neo4j.driver.Entity;
import org.neo4j.driver.Node;
import org.neo4j.driver.Path;
import org.neo4j.driver.Relationship;

/**
 * {@link org.neo4j.driver.Path} implementation that directly contains all nodes and relationships.
 */
public class SimplePath implements Path
{
    public static class SelfContainedSegment implements Segment
    {
        private final Node start;
        private final Relationship relationship;
        private final Node end;

        public SelfContainedSegment( Node start, Relationship relationship, Node end )
        {
            this.start = start;
            this.relationship = relationship;
            this.end = end;
        }

        @Override
        public Node start()
        {
            return start;
        }

        @Override
        public Relationship relationship()
        {
            return relationship;
        }

        @Override
        public Node end()
        {
            return end;
        }

        @Override
        public boolean equals( Object other )
        {
            if ( this == other )
            {
                return true;
            }
            if ( other == null || getClass() != other.getClass() )
            {
                return false;
            }

            SelfContainedSegment that = (SelfContainedSegment) other;

            return start.equals( that.start ) && end.equals( that.end ) && relationship.equals( that.relationship );

        }

        @Override
        public int hashCode()
        {
            int result = start.hashCode();
            result = 31 * result + relationship.hashCode();
            result = 31 * result + end.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return String.format( relationship.start().equals( start.identity() ) ?
                                  "(%s)-[%s:%s]->(%s)" : "(%s)<-[%s:%s]-(%s)",
                    start.identity(), relationship.identity(), relationship.type(), end.identity() );
        }
    }

    private static boolean isEndpoint( Node node, Relationship relationship )
    {
        return node.identity().equals( relationship.start() ) || node.identity().equals( relationship.end() );
    }

    private final ArrayList<Node> nodes = new ArrayList<>();
    private final ArrayList<Relationship> relationships = new ArrayList<>();
    private final ArrayList<Segment> segments = new ArrayList<>();

    public SimplePath( List<Entity> alternatingNodeAndRel )
    {
        if ( alternatingNodeAndRel.size() % 2 == 0 )
        {
            throw new IllegalArgumentException( "An odd number of entities are required to build a path" );
        }
        Node lastNode = null;
        Relationship lastRelationship = null;
        int index = 0;
        for ( Entity entity : alternatingNodeAndRel )
        {
            if ( entity == null )
            {
                throw new IllegalArgumentException( "Path entities cannot be null" );
            }
            if ( index % 2 == 0 )
            {
                // even index - this should be a node
                try
                {
                    lastNode = (Node) entity;
                    if ( nodes.isEmpty() || isEndpoint( lastNode, lastRelationship ) )
                    {
                        nodes.add( lastNode );
                    }
                    else
                    {
                        throw new IllegalArgumentException(
                                "Node argument " + index + " is not an endpoint of relationship argument " + (index -
                                                                                                              1) );
                    }
                }
                catch ( ClassCastException e )
                {
                    String cls = entity.getClass().getName();
                    throw new IllegalArgumentException(
                            "Expected argument " + index + " to be a node " + index + " but found a " + cls + " " +
                            "instead" );
                }
            }
            else
            {
                // odd index - this should be a relationship
                try
                {
                    lastRelationship = (Relationship) entity;
                    if ( isEndpoint( lastNode, lastRelationship ) )
                    {
                        relationships.add( lastRelationship );
                    }
                    else
                    {
                        throw new IllegalArgumentException(
                                "Node argument " + (index - 1) + " is not an endpoint of relationship argument " +
                                index );
                    }
                }
                catch ( ClassCastException e )
                {
                    String cls = entity.getClass().getName();
                    throw new IllegalArgumentException(
                            "Expected argument " + index + " to be a relationship but found a " + cls + " instead" );
                }
            }
            index += 1;
        }
        buildSegments();
    }

    public SimplePath( Entity... alternatingNodeAndRel )
    {
        this( Arrays.asList( alternatingNodeAndRel ) );
    }

    @Override
    public long length()
    {
        return relationships.size();
    }

    @Override
    public boolean contains( Node node )
    {
        return nodes.contains( node );
    }

    @Override
    public boolean contains( Relationship relationship )
    {
        return relationships.contains( relationship );
    }

    @Override
    public Iterable<Node> nodes()
    {
        return nodes;
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        return relationships;
    }

    @Override
    public Node start()
    {
        return nodes.get( 0 );
    }

    @Override
    public Node end()
    {
        return nodes.get( nodes.size() - 1 );
    }

    @Override
    public Iterator<Segment> iterator()
    {
        return segments.iterator();
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

        SimplePath segments1 = (SimplePath) o;

        return segments.equals( segments1.segments );

    }

    @Override
    public int hashCode()
    {
        return segments.hashCode();
    }

    @Override
    public String toString()
    {

        return "Path[" + segments + ']';
    }

    private void buildSegments()
    {
        for ( int i = 0; i < relationships.size(); i++ )
        {
            segments.add( new SelfContainedSegment( nodes.get( i ), relationships.get( i ), nodes.get( i + 1 ) ) );
        }
    }
}
