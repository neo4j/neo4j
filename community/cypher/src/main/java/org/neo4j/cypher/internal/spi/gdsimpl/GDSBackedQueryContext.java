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
package org.neo4j.cypher.internal.spi.gdsimpl;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import org.neo4j.cypher.internal.spi.QueryContext;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class GDSBackedQueryContext implements QueryContext
{
    private final GraphDatabaseService graph;

    public GDSBackedQueryContext( GraphDatabaseService graph )
    {
        this.graph = graph;
    }

    @Override
    public void close()
    {
    }

    @Override
    public Node createNode()
    {
        return graph.createNode();
    }

    @Override
    public Relationship createRelationship( Node start, Node end, String relType )
    {
        return start.createRelationshipTo( end, withName( relType ) );
    }

    @Override
    public Iterable<Relationship> getRelationshipsFor( Node node, Direction dir, String... types )
    {
        if ( types.length == 0 )
        {
            return node.getRelationships( dir );
        }
        else
        {
            return node.getRelationships( dir, transform( types ) );
        }
    }

    @Override
    public void addLabelsToNode(Node node, Iterable<Long> labelIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getOrCreateLabelId(String labelName) {
        throw new UnsupportedOperationException();
    }

    private RelationshipType[] transform( String[] types )
    {
        RelationshipType[] result = new RelationshipType[types.length];
        for ( int i = 0; i < types.length; i++ )
        {
            result[i] = withName( types[i] );
        }

        return result;
    }

    @Override
    public Operations<Node> nodeOps()
    {
        return new Operations<Node>()
        {
            @Override
            public void delete( Node obj )
            {
                obj.delete();
            }

            @Override
            public void setProperty( Node obj, String propertyKey, Object value )
            {
                obj.setProperty( propertyKey, value );
            }

            @Override
            public void removeProperty( Node obj, String propertyKey )
            {
                obj.removeProperty( propertyKey );
            }

            @Override
            public Object getProperty( Node obj, String propertyKey )
            {
                return obj.getProperty( propertyKey );
            }

            @Override
            public boolean hasProperty( Node obj, String propertyKey )
            {
                return obj.hasProperty( propertyKey );
            }

            @Override
            public Iterable<String> propertyKeys( Node obj )
            {
                return obj.getPropertyKeys();
            }

            @Override
            public Node getById( long id )
            {
                return graph.getNodeById( id );
            }
        };
    }

    @Override
    public Operations<Relationship> relationshipOps()
    {
        return new Operations<Relationship>()
        {
            @Override
            public void delete( Relationship obj )
            {
                obj.delete();
            }

            @Override
            public void setProperty( Relationship obj, String propertyKey, Object value )
            {
                obj.setProperty( propertyKey, value );
            }

            @Override
            public void removeProperty( Relationship obj, String propertyKey )
            {
                obj.removeProperty( propertyKey );
            }

            @Override
            public Object getProperty( Relationship obj, String propertyKey )
            {
                return obj.getProperty( propertyKey );
            }

            @Override
            public boolean hasProperty( Relationship obj, String propertyKey )
            {
                return obj.hasProperty( propertyKey );
            }

            @Override
            public Iterable<String> propertyKeys( Relationship obj )
            {
                return obj.getPropertyKeys();
            }

            @Override
            public Relationship getById( long id )
            {
                return graph.getRelationshipById( id );
            }
        };
    }
}
