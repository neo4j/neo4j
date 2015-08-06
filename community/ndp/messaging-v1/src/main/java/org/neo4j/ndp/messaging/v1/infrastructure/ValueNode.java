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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.ndp.messaging.v1.Neo4jPack;

public class ValueNode implements Node
{
    private static final int STRUCT_FIELD_COUNT = 3;

    public static void pack( Neo4jPack.Packer packer, Node node )
            throws IOException
    {
        packer.packStructHeader( STRUCT_FIELD_COUNT, Neo4jPack.NODE );
        packer.packNodeIdentity( node.getId() );
        Collection<Label> collectedLabels = Iterables.toList( node.getLabels() );
        packer.packListHeader( collectedLabels.size() );
        for ( Label label : collectedLabels )
        {
            packer.pack( label.name() );
        }
        packer.packProperties( node );
    }

    public static ValueNode unpack( Neo4jPack.Unpacker unpacker )
            throws IOException
    {
        assert unpacker.unpackStructHeader() == STRUCT_FIELD_COUNT;
        assert unpacker.unpackStructSignature() == Neo4jPack.NODE;
        return unpackFields( unpacker );
    }

    public static ValueNode unpackFields( Neo4jPack.Unpacker unpacker )
            throws IOException
    {
        long id = unpacker.unpackNodeIdentity();

        int numLabels = (int) unpacker.unpackListHeader();
        List<Label> labels;
        if ( numLabels > 0 )
        {
            labels = new ArrayList<>( numLabels );
            for ( int i = 0; i < numLabels; i++ )
            {
                labels.add( DynamicLabel.label( unpacker.unpackText() ) );
            }
        }
        else
        {
            labels = Collections.emptyList();
        }

        Map<String, Object> props = unpacker.unpackProperties();

        return new ValueNode( id, labels, props );
    }

    private final long id;
    private final Collection<Label> labels;
    private final Map<String,Object> props;

    public ValueNode( long id, Collection<Label> labels, Map<String,Object> props )
    {
        this.id = id;
        this.labels = labels;
        this.props = props;
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
    public Object getProperty( String s )
    {
        return props.get( s );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        return props.keySet();
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
    public Traverser traverse( Traverser.Order order, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, RelationshipType relationshipType, Direction direction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Traverser traverse( Traverser.Order order, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, RelationshipType relationshipType, Direction direction,
            RelationshipType relationshipType2, Direction direction2 )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Traverser traverse( Traverser.Order order, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, Object... objects )
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
        result = 31 * result + props.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "ValueNode{" +
               "id=" + id +
               ", labels=" + labels +
               ", props=" + props +
               '}';
    }

}
