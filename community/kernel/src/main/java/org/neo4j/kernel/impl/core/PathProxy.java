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
package org.neo4j.kernel.impl.core;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Paths;

import static org.neo4j.helpers.collection.Iterators.iteratorsEqual;

public class PathProxy implements Path
{
    private final EmbeddedProxySPI proxySPI;
    private final long[] nodes;
    private final long[] relationships;
    private final int[] directedTypes;

    /**
     * @param proxySPI
     *         the API into the kernel.
     * @param nodes
     *         the ids of the nodes in the path, in order.
     * @param relationships
     *         the ids of the relationships in the path, in order.
     * @param directedTypes
     *         an encoding of the types and directions of the relationships.
     *         An entry at position {@code i} of this array should be {@code typeId} if the relationship at {@code i}
     *         has its start node at {@code i} and its end node at {@code i + 1}, and should be {@code ~typeId} if the
     *         relationship at {@code i} has its start node at {@code i + 1} and its end node at {@code i}.
     */
    public PathProxy( EmbeddedProxySPI proxySPI, long[] nodes, long[] relationships, int[] directedTypes )
    {
        assert nodes.length == relationships.length + 1;
        assert relationships.length == directedTypes.length;
        this.proxySPI = proxySPI;
        this.nodes = nodes;
        this.relationships = relationships;
        this.directedTypes = directedTypes;
    }

    @Override
    public String toString()
    {
        StringBuilder string = new StringBuilder();
        string.append( '(' ).append( nodes[0] ).append( ')' );
        boolean inTx = true;
        for ( int i = 0; i < relationships.length; i++ )
        {
            int type = directedTypes[i];
            string.append( type < 0 ? "<-[" : "-[" );
            string.append( relationships[i] );
            if ( inTx )
            {
                try
                {
                    String name = proxySPI.getRelationshipTypeById( type < 0 ? ~type : type ).name();
                    string.append( ':' ).append( name );
                }
                catch ( Exception e )
                {
                    inTx = false;
                }
            }
            string.append( type < 0 ? "]-(" : "]->(" ).append( nodes[i + 1] ).append( ')' );
        }
        return string.toString();
    }

    @Override
    public int hashCode()
    {
        if ( relationships.length == 0 )
        {
            return Long.hashCode( nodes[0] );
        }
        else
        {
            return Arrays.hashCode( relationships );
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj instanceof PathProxy )
        {
            PathProxy that = (PathProxy) obj;
            return Arrays.equals( this.nodes, that.nodes ) && Arrays.equals( this.relationships, that.relationships );
        }
        else if ( obj instanceof Path )
        {
            Path other = (Path) obj;
            if ( nodes[0] != other.startNode().getId() )
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
    public Node startNode()
    {
        return new NodeProxy( proxySPI, nodes[0] );
    }

    @Override
    public Node endNode()
    {
        return new NodeProxy( proxySPI, nodes[nodes.length - 1] );
    }

    @Override
    public Relationship lastRelationship()
    {
        return relationships.length == 0
                ? null
                : relationship( relationships.length - 1 );
    }

    private RelationshipProxy relationship( int offset )
    {
        int type = directedTypes[offset];
        if ( type >= 0 )
        {
            return new RelationshipProxy( proxySPI, relationships[offset], nodes[offset], type, nodes[offset + 1] );
        }
        else
        {
            return new RelationshipProxy( proxySPI, relationships[offset], nodes[offset + 1], ~type, nodes[offset] );
        }
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        return () -> new Iterator<Relationship>()
        {
            int i;

            @Override
            public boolean hasNext()
            {
                return i < relationships.length;
            }

            @Override
            public Relationship next()
            {
                return relationship( i++ );
            }
        };
    }

    @Override
    public Iterable<Relationship> reverseRelationships()
    {
        return () -> new Iterator<Relationship>()
        {
            int i = relationships.length;

            @Override
            public boolean hasNext()
            {
                return i > 0;
            }

            @Override
            public Relationship next()
            {
                return relationship( --i );
            }
        };
    }

    @Override
    public Iterable<Node> nodes()
    {
        return () -> new Iterator<Node>()
        {
            int i;

            @Override
            public boolean hasNext()
            {
                return i < nodes.length;
            }

            @Override
            public Node next()
            {
                return new NodeProxy( proxySPI, nodes[i++] );
            }
        };
    }

    @Override
    public Iterable<Node> reverseNodes()
    {
        return () -> new Iterator<Node>()
        {
            int i = nodes.length;

            @Override
            public boolean hasNext()
            {
                return i > 0;
            }

            @Override
            public Node next()
            {
                return new NodeProxy( proxySPI, nodes[--i] );
            }
        };
    }

    @Override
    public int length()
    {
        return relationships.length;
    }

    @Override
    public Iterator<PropertyContainer> iterator()
    {
        return new Iterator<PropertyContainer>()
        {
            int i;
            boolean relationship;

            @Override
            public boolean hasNext()
            {
                return i < relationships.length || !relationship;
            }

            @Override
            public PropertyContainer next()
            {
                if ( relationship )
                {
                    relationship = false;
                    return relationship( i++ );
                }
                else
                {
                    relationship = true;
                    return new NodeProxy( proxySPI, nodes[i] );
                }
            }
        };
    }
}
