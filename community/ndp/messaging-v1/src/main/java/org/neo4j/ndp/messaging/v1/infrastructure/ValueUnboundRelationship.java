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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.ndp.messaging.v1.Neo4jPack;

public class ValueUnboundRelationship implements UnboundRelationship
{
    private static final int STRUCT_FIELD_COUNT = 3;

    public static void pack( Neo4jPack.Packer packer, Relationship rel )
            throws IOException
    {
        packer.packStructHeader( STRUCT_FIELD_COUNT, Neo4jPack.UNBOUND_RELATIONSHIP );
        packer.packRelationshipIdentity( rel.getId() );
        packer.pack( rel.getType().name() );
        packer.packProperties( rel );
    }

    public static ValueUnboundRelationship unpack( Neo4jPack.Unpacker unpacker )
            throws IOException
    {
        assert unpacker.unpackStructHeader() == STRUCT_FIELD_COUNT;
        assert unpacker.unpackStructSignature() == Neo4jPack.UNBOUND_RELATIONSHIP;
        return unpackFields( unpacker );
    }

    public static ValueUnboundRelationship unpackFields( Neo4jPack.Unpacker unpacker )
            throws IOException
    {
        long relId = unpacker.unpackRelationshipIdentity();
        String relTypeName = unpacker.unpackText();

        Map<String, Object> props = unpacker.unpackProperties();

        RelationshipType relType = DynamicRelationshipType.withName( relTypeName );

        return new ValueUnboundRelationship( relId, relType, props );
    }

    public static ValueUnboundRelationship unbind( Relationship relationship ) {
        Map<String, Object> props = new HashMap<>();
        for(String key: relationship.getPropertyKeys()) {
            props.put( key, relationship.getProperty( key ) );
        }
        return new ValueUnboundRelationship( relationship.getId(), relationship.getType(), props );
    }

    private final long id;
    private final RelationshipType type;
    private final Map<String,Object> props;

    public ValueUnboundRelationship( long id, RelationshipType type, Map<String, Object> props )
    {
        this.id = id;
        this.type = type;
        this.props = props;
    }

    public long getId()
    {
        return id;
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
    public GraphDatabaseService getGraphDatabase()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasProperty( String key )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty( String key, Object value )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object removeProperty( String key )
    {
        throw new UnsupportedOperationException();
    }

    public RelationshipType getType()
    {
        return type;
    }

    @Override
    public boolean isType( RelationshipType type )
    {
        return false;
    }

    @Override
    public String toString()
    {
        return "ValueUnboundRelationship{" +
                "id=" + id +
                ", type=" + type +
                ", props=" + props +
                '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) return true;
        if ( !(o instanceof ValueUnboundRelationship) ) return false;

        ValueUnboundRelationship that = (ValueUnboundRelationship) o;

        if ( id != that.id ) return false;
        if ( props != null ? !props.equals( that.props ) : that.props != null ) return false;
        if ( type != null ? !type.equals( that.type ) : that.type != null ) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (props != null ? props.hashCode() : 0);
        return result;
    }

    @Override
    public Relationship bind( Node startNode, Node endNode )
    {
        return new ValueRelationship( id, startNode.getId(), endNode.getId(), type, props );
    }

    @Override
    public void delete()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node getStartNode()
    {
        return null;
    }

    @Override
    public Node getEndNode()
    {
        return null;
    }

    @Override
    public Node getOtherNode( Node node )
    {
        return null;
    }

    @Override
    public Node[] getNodes()
    {
        return new Node[0];
    }

}
