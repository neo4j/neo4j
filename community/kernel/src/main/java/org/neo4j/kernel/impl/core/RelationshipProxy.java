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
import org.neo4j.kernel.api.exceptions.ReadOnlyDatabaseKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;

public class RelationshipProxy implements Relationship
{
    public interface RelationshipLookups
    {
        Node newNodeProxy( long nodeId );
        RelationshipImpl lookupRelationship(long relationshipId);
        GraphDatabaseService getGraphDatabaseService();
        NodeManager getNodeManager();
    }

    private final long relId;
    private final RelationshipLookups relationshipLookups;
    private final ThreadToStatementContextBridge statementContextProvider;

    RelationshipProxy( long relId, RelationshipLookups relationshipLookups,
                       ThreadToStatementContextBridge statementContextProvider )
    {
        this.relId = relId;
        this.relationshipLookups = relationshipLookups;
        this.statementContextProvider = statementContextProvider;
    }

    @Override
    public long getId()
    {
        return relId;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return relationshipLookups.getGraphDatabaseService();
    }

    @Override
    public void delete()
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            statement.dataWriteOperations().relationshipDelete( getId() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    @Override
    public Node[] getNodes()
    {
        assertInTransaction();
        RelationshipImpl relationship = relationshipLookups.lookupRelationship( relId );
        return new Node[]{ relationshipLookups.newNodeProxy( relationship.getStartNodeId() ), relationshipLookups.newNodeProxy( relationship.getEndNodeId() )};
    }

    @Override
    public Node getOtherNode( Node node )
    {
        assertInTransaction();
        RelationshipImpl relationship = relationshipLookups.lookupRelationship( relId );
        if ( relationship.getStartNodeId() == node.getId() )
        {
            return relationshipLookups.newNodeProxy( relationship.getEndNodeId() );
        }
        if ( relationship.getEndNodeId() == node.getId() )
        {
            return relationshipLookups.newNodeProxy( relationship.getStartNodeId() );
        }
        throw new NotFoundException( "Node[" + node.getId()
            + "] not connected to this relationship[" + getId() + "]" );
    }

    @Override
    public Node getStartNode()
    {
        assertInTransaction();
        return relationshipLookups.newNodeProxy( relationshipLookups.lookupRelationship( relId ).getStartNodeId() );
    }

    @Override
    public Node getEndNode()
    {
        assertInTransaction();
        return relationshipLookups.newNodeProxy( relationshipLookups.lookupRelationship( relId ).getEndNodeId() );
    }

    @Override
    public RelationshipType getType()
    {
        assertInTransaction();
        try
        {
            return relationshipLookups.getNodeManager().getRelationshipTypeById( relationshipLookups.lookupRelationship( relId )
                                                                                     .getTypeId() );
        }
        catch ( TokenNotFoundException e )
        {
            throw new NotFoundException( e );
        }
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        try ( Statement statement = statementContextProvider.instance() )
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

        try ( Statement statement = statementContextProvider.instance() )
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

        try ( Statement statement = statementContextProvider.instance() )
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

        try ( Statement statement = statementContextProvider.instance() )
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
        boolean success = false;
        try ( Statement statement = statementContextProvider.instance() )
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            statement.dataWriteOperations().relationshipSetProperty( relId, Property.property( propertyKeyId, value ) );
            success = true;
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
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
        finally
        {
            if ( !success )
            {
                relationshipLookups.getNodeManager().setRollbackOnly();
            }
        }
    }

    @Override
    public Object removeProperty( String key )
    {
        try ( Statement statement = statementContextProvider.instance() )
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
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    @Override
    public boolean isType( RelationshipType type )
    {
        assertInTransaction();
        try
        {
            return relationshipLookups.getNodeManager().getRelationshipTypeById(
                    relationshipLookups.lookupRelationship( relId ).getTypeId() ).name().equals( type.name() );
        }
        catch ( TokenNotFoundException e )
        {
            throw new NotFoundException( e );
        }
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
        return (int) (( relId >>> 32 ) ^ relId );
    }

    @Override
    public String toString()
    {
        return "Relationship[" + this.getId() + "]";
    }

    private void assertInTransaction()
    {
        statementContextProvider.assertInTransaction();
    }
}
