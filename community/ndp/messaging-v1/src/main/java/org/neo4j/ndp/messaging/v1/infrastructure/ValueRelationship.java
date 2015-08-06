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

import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.ndp.messaging.v1.Neo4jPack;

public class ValueRelationship implements Relationship
{
    private static final int STRUCT_FIELD_COUNT = 5;

    public static void pack( Neo4jPack.Packer packer, Relationship rel )
            throws IOException
    {
        packer.packStructHeader( STRUCT_FIELD_COUNT, Neo4jPack.RELATIONSHIP );
        packer.packRelationshipIdentity( rel.getId() );
        packer.packNodeIdentity( rel.getStartNode().getId() );
        packer.packNodeIdentity( rel.getEndNode().getId() );
        packer.pack( rel.getType().name() );
        packer.packProperties( rel );
    }

    public static ValueRelationship unpack( Neo4jPack.Unpacker unpacker )
            throws IOException
    {
        assert unpacker.unpackStructHeader() == STRUCT_FIELD_COUNT;
        assert unpacker.unpackStructSignature() == Neo4jPack.RELATIONSHIP;
        return unpackFields( unpacker );
    }

    public static ValueRelationship unpackFields( Neo4jPack.Unpacker unpacker )
            throws IOException
    {
        long relId = unpacker.unpackRelationshipIdentity();
        long startNodeId = unpacker.unpackNodeIdentity();
        long endNodeId = unpacker.unpackNodeIdentity();
        String relTypeName = unpacker.unpackText();

        Map<String, Object> props = unpacker.unpackProperties();

        RelationshipType relType = DynamicRelationshipType.withName( relTypeName );

        return new ValueRelationship( relId, startNodeId, endNodeId, relType, props );
    }

    private final long id;
    private final long startNode;
    private final long endNode;
    private final RelationshipType type;
    private final Map<String,Object> props;

    public ValueRelationship( long id, long from, long to, RelationshipType type, Map<String,Object> props )
    {
        this.id = id;
        this.startNode = from;
        this.endNode = to;
        this.type = type;
        this.props = props;
    }

    @Override
    public long getId()
    {
        return id;
    }

    @Override
    public Node getStartNode()
    {
        return new ValueNode( startNode, null, null );
    }

    @Override
    public Node getEndNode()
    {
        return new ValueNode( endNode, null, null );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        return props.keySet();
    }

    @Override
    public Object getProperty( String s )
    {
        return props.get( s );
    }

    @Override
    public RelationshipType getType()
    {
        return type;
    }

    @Override
    public Node getOtherNode( Node node )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node[] getNodes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isType( RelationshipType relationshipType )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasProperty( String s )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getProperty( String s, Object o )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty( String s, Object o )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object removeProperty( String s )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "ValueRelationship{" +
               "id=" + id +
               ", startNode=" + startNode +
               ", endNode=" + endNode +
               ", type=" + type +
               ", props=" + props +
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

        ValueRelationship that = (ValueRelationship) o;

        if ( endNode != that.endNode )
        {
            return false;
        }
        if ( id != that.id )
        {
            return false;
        }
        return startNode == that.startNode &&
               !(type != null ? !type.name().equals( that.type.name() ) : that.type != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (int) (startNode ^ (startNode >>> 32));
        result = 31 * result + (int) (endNode ^ (endNode >>> 32));
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }
}
