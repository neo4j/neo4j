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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.BoltIOException;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.Status;

public class ValueNode
        extends ValuePropertyContainer
        implements Node
{
    private static final int STRUCT_FIELD_COUNT = 3;

    public static void pack( Neo4jPack.Packer packer, Node node )
            throws IOException
    {
        //TODO: We should mark deleted nodes properly but that requires updates to protocol and
        //clients. Until that we merely don't fail and return a node with neither labels nor properties
        packer.packStructHeader( STRUCT_FIELD_COUNT, Neo4jPack.NODE );
        packer.pack( node.getId() );
        try
        {
            //read labels and properties, will fail if node has been deleted
            Collection<Label> collectedLabels = Iterables.asList( node.getLabels() );
            Map<String, Object> props = node.getAllProperties();

            packer.packListHeader( collectedLabels.size() );
            for ( Label label : collectedLabels )
            {
                packer.pack( label.name() );
            }
            packer.packRawMap( props );
        }
        catch ( NotFoundException e )
        {
            //node is deleted, just send along an empty node
            packer.packListHeader( 0 );
            packer.packRawMap( Collections.emptyMap() );
        }
    }

    public static ValueNode unpack( Neo4jPack.Unpacker unpacker )
            throws IOException
    {
        long numFields = unpacker.unpackStructHeader();
        char signature = unpacker.unpackStructSignature();
        if( signature != Neo4jPack.NODE ) {
            throw new BoltIOException( Status.Request.InvalidFormat, "Expected a node structure, received 0x" + Integer.toHexString( signature ) );
        }
        if( numFields != STRUCT_FIELD_COUNT ) {
            throw new BoltIOException( Status.Request.InvalidFormat, "Node structures should have " + STRUCT_FIELD_COUNT
                                                                     + " fields, structure sent contained " + numFields );
        }
        return unpackFields( unpacker );
    }

    public static ValueNode unpackFields( Neo4jPack.Unpacker unpacker )
            throws IOException
    {
        long id = unpacker.unpackLong();

        int numLabels = (int) unpacker.unpackListHeader();
        List<Label> labels;
        if ( numLabels > 0 )
        {
            labels = new ArrayList<>( numLabels );
            for ( int i = 0; i < numLabels; i++ )
            {
                labels.add( Label.label( unpacker.unpackString() ) );
            }
        }
        else
        {
            labels = Collections.emptyList();
        }

        Map<String, Object> props = unpacker.unpackMap();

        return new ValueNode( id, labels, props );
    }

    private final long id;
    private final Collection<Label> labels;

    public ValueNode( long id, Collection<Label> labels, Map<String,Object> props )
    {
        super( props );

        this.id = id;
        this.labels = labels;
    }

    @Override
    public long getId()
    {
        return id;
    }

    @Override
    public Iterable<Label> getLabels()
    {
        return labels;
    }

    @Override
    public void delete()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> getRelationships()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasRelationship()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType... relationshipTypes )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction direction, RelationshipType... relationshipTypes )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasRelationship( RelationshipType... relationshipTypes )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasRelationship( Direction direction, RelationshipType... relationshipTypes )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction direction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasRelationship( Direction direction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType relationshipType, Direction direction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasRelationship( RelationshipType relationshipType, Direction direction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Relationship getSingleRelationship( RelationshipType relationshipType, Direction direction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Relationship createRelationshipTo( Node node, RelationshipType relationshipType )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDegree()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDegree( RelationshipType relationshipType )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDegree( Direction direction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDegree( RelationshipType relationshipType, Direction direction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addLabel( Label label )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeLabel( Label label )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasLabel( Label label )
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

        ValueNode valueNode = (ValueNode) o;

        return id == valueNode.id && labels.equals( valueNode.labels );

    }

    @Override
    public int hashCode()
    {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + labels.hashCode();
        result = 31 * result + getAllProperties().hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "ValueNode{" +
               "id=" + id +
               ", labels=" + labels +
                ", props=" + getAllProperties() +
               '}';
    }

}
