/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.messaging.infrastructure;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class ValueUnboundRelationship
        extends ValuePropertyContainer
        implements UnboundRelationship
{
    private static final int STRUCT_FIELD_COUNT = 3;

    public static void pack( Neo4jPack.Packer packer, Relationship rel )
            throws IOException
    {
        packer.packStructHeader( STRUCT_FIELD_COUNT, Neo4jPack.UNBOUND_RELATIONSHIP );
        packer.pack( rel.getId() );
        packer.pack( rel.getType().name() );
        //TODO: We should mark deleted relationships properly but that requires updates
        // to protocol and clients.
        try{
            Map<String,Object> properties = rel.getAllProperties();
            packer.packRawMap( properties );
        }
        catch(NotFoundException e)
        {
            //relationship was deleted, just send empty property map back
            packer.packRawMap( Collections.emptyMap() );
        }
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
        long relId = unpacker.unpackLong();
        String relTypeName = unpacker.unpackString();

        Map<String, Object> props = unpacker.unpackMap();

        RelationshipType relType = RelationshipType.withName( relTypeName );

        return new ValueUnboundRelationship( relId, relType, props );
    }

    public static ValueUnboundRelationship unbind( Relationship relationship )
    {
        Map<String, Object> props = relationship.getAllProperties();
        return new ValueUnboundRelationship( relationship.getId(), relationship.getType(), props );
    }

    private final long id;
    private final RelationshipType type;

    public ValueUnboundRelationship( long id, RelationshipType type, Map<String, Object> props )
    {
        super( props );
        this.id = id;
        this.type = type;
    }

    public long getId()
    {
        return id;
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
                ", props=" + getAllProperties() +
                '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) return true;
        if ( !(o instanceof ValueUnboundRelationship) ) return false;

        ValueUnboundRelationship that = (ValueUnboundRelationship) o;

        if ( id != that.id ) return false;
        if ( getAllProperties() != null ? !getAllProperties().equals(
                that.getAllProperties() ) : that.getAllProperties() != null )
        {
            return false;
        }
        if ( type != null ? !type.equals( that.type ) : that.type != null ) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (getAllProperties() != null ? getAllProperties().hashCode() : 0);
        return result;
    }

    @Override
    public Relationship bind( Node startNode, Node endNode )
    {
        return new ValueRelationship( id, startNode.getId(), endNode.getId(), type, getAllProperties() );
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
