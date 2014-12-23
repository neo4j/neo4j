/**
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
package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;

public class RelationshipProxy implements Relationship
{
    public interface RelationshipActions
    {
        Statement statement();
        
        Node newNodeProxy( long nodeId );

        RelationshipData getRelationshipData( long relationshipId );

        RelationshipType getRelationshipTypeById( int type );

        GraphDatabaseService getGraphDatabaseService();

        void failTransaction();

        void assertInUnterminatedTransaction();
    }

    private final RelationshipActions actions;
    private final long relId;

    public RelationshipProxy( RelationshipActions actions, long relId )
    {
        this.relId = relId;
        this.actions = actions;
    }

    @Override
    public long getId()
    {
        return relId;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return actions.getGraphDatabaseService();
    }

    @Override
    public void delete()
    {
        try ( Statement statement = actions.statement() )
        {
            statement.dataWriteOperations().relationshipDelete( getId() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( "Unable to delete relationship[" +
                                             relId + "] since it is already deleted." );
        }
    }

    @Override
    public Node[] getNodes()
    {
        assertInUnterminatedTransaction();
        RelationshipData data = actions.getRelationshipData( relId );
        return new Node[]{
                actions.newNodeProxy( data.getStartNode() ),
                actions.newNodeProxy( data.getEndNode() )};
    }

    @Override
    public Node getOtherNode( Node node )
    {
        assertInUnterminatedTransaction();
        RelationshipData data = actions.getRelationshipData( relId );
        if ( data.getStartNode() == node.getId() )
        {
            return actions.newNodeProxy( data.getEndNode() );
        }
        if ( data.getEndNode() == node.getId() )
        {
            return actions.newNodeProxy( data.getStartNode() );
        }
        throw new NotFoundException( "Node[" + node.getId()
                                     + "] not connected to this relationship[" + getId() + "]" );
    }

    @Override
    public Node getStartNode()
    {
        assertInUnterminatedTransaction();
        return actions.newNodeProxy( actions.getRelationshipData( relId ).getStartNode() );
    }

    @Override
    public Node getEndNode()
    {
        assertInUnterminatedTransaction();
        return actions.newNodeProxy( actions.getRelationshipData( relId ).getEndNode() );
    }

    @Override
    public RelationshipType getType()
    {
        assertInUnterminatedTransaction();
        int type = actions.getRelationshipData( relId ).getType();
        return actions.getRelationshipTypeById( type );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        try ( Statement statement = actions.statement() )
        {
            List<String> keys = new ArrayList<>();
            Iterator<DefinedProperty> properties = statement.readOperations().relationshipGetAllProperties( getId() );
            while ( properties.hasNext() )
            {
                keys.add( statement.readOperations().propertyKeyGetName( properties.next().propertyKeyId() ) );
            }
            return keys;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Relationship not found", e );
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new ThisShouldNotHappenError( "Jake",
                    "Property key retrieved through kernel API should exist." );
        }
    }

    @Override
    public Object getProperty( String key )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }

        try ( Statement statement = actions.statement() )
        {
            try
            {
                int propertyId = statement.readOperations().propertyKeyGetForName( key );
                if ( propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
                {
                    throw new NotFoundException( String.format( "No such property, '%s'.", key ) );
                }
                return statement.readOperations().relationshipGetProperty( relId, propertyId ).value();
            }
            catch ( EntityNotFoundException | PropertyNotFoundException e )
            {
                throw new NotFoundException(
                        e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            }
        }
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }

        try ( Statement statement = actions.statement() )
        {
            int propertyId = statement.readOperations().propertyKeyGetForName( key );
            return statement.readOperations().relationshipGetProperty( relId, propertyId ).value( defaultValue );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
        }
    }

    @Override
    public boolean hasProperty( String key )
    {
        if ( null == key )
        {
            return false;
        }

        try ( Statement statement = actions.statement() )
        {
            int propertyId = statement.readOperations().propertyKeyGetForName( key );
            return propertyId != KeyReadOperations.NO_SUCH_PROPERTY_KEY &&
                   statement.readOperations().relationshipGetProperty( relId, propertyId ).isDefined();
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
        }
    }

    @Override
    public void setProperty( String key, Object value )
    {
        try ( Statement statement = actions.statement() )
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            statement.dataWriteOperations().relationshipSetProperty( relId, Property.property( propertyKeyId, value ) );
        }
        catch ( IllegalArgumentException e )
        {
            // Trying to set an illegal value is a critical error - fail this transaction
            actions.failTransaction();
            throw e;
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( IllegalTokenNameException e )
        {
            // TODO: Maybe throw more context-specific error than just IllegalArgument
            throw new IllegalArgumentException( e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Object removeProperty( String key )
    {
        try ( Statement statement = actions.statement() )
        {
            int propertyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            return statement.dataWriteOperations().relationshipRemoveProperty( relId, propertyId ).value( null );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( IllegalTokenNameException e )
        {
            // TODO: Maybe throw more context-specific error than just IllegalArgument
            throw new IllegalArgumentException( e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public boolean isType( RelationshipType type )
    {
        assertInUnterminatedTransaction();
        int typeId = actions.getRelationshipData( relId ).getType();
        return actions.getRelationshipTypeById( typeId ).name().equals( type.name() );
    }

    public int compareTo( Object rel )
    {
        Relationship r = (Relationship) rel;
        long ourId = this.getId(), theirId = r.getId();

        if ( ourId < theirId )
        {
            return -1;
        }
        else if ( ourId > theirId )
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }

    @Override
    public boolean equals( Object o )
    {
        return o instanceof Relationship && this.getId() == ((Relationship) o).getId();
    }

    @Override
    public int hashCode()
    {
        return (int) ((relId >>> 32) ^ relId);
    }

    @Override
    public String toString()
    {
        return "Relationship[" + this.getId() + "]";
    }

    private void assertInUnterminatedTransaction()
    {
        actions.assertInUnterminatedTransaction();
    }
}
