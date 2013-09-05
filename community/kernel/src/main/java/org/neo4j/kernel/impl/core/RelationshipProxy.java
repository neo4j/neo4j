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
package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.DataStatement;
import org.neo4j.kernel.api.ReadStatement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.operations.KeyReadOperations;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.SafeProperty;

import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

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
    private final ThreadToStatementContextBridge statementCtxProvider;

    RelationshipProxy( long relId, RelationshipLookups relationshipLookups,
                       ThreadToStatementContextBridge statementCtxProvider )
    {
        this.relId = relId;
        this.relationshipLookups = relationshipLookups;
        this.statementCtxProvider = statementCtxProvider;
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
        try ( DataStatement statement = statementCtxProvider.dataStatement() )
        {
            statement.relationshipDelete( getId() );
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
        try ( ReadStatement statement = statementCtxProvider.readStatement() )
        {
            List<String> keys = new ArrayList<>();
            Iterator<SafeProperty> properties = statement.relationshipGetAllProperties( getId() );
            while ( properties.hasNext() )
            {
                keys.add( statement.propertyKeyGetName( properties.next().propertyKeyId() ) );
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
    public Iterable<Object> getPropertyValues()
    {
        try ( ReadStatement statement = statementCtxProvider.readStatement() )
        {
            return asSet( map( new Function<SafeProperty, Object>()
            {
                @Override
                public Object apply( SafeProperty prop )
                {
                    return prop.value();
                }
            }, statement.relationshipGetAllProperties( getId() ) ) );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Relationship has been deleted", e );
        }
    }

    @Override
    public Object getProperty( String key )
    {
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        try ( ReadStatement statement = statementCtxProvider.readStatement() )
        {
            long propertyId = statement.propertyKeyGetForName( key );
            if ( propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
            {
                throw new NotFoundException( String.format( "No such property, '%s'.", key ) );
            }
            return statement.relationshipGetProperty( relId, propertyId ).value();
        }
        catch ( EntityNotFoundException | PropertyNotFoundException e )
        {
            throw new NotFoundException( e );
        }
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        try ( ReadStatement statement = statementCtxProvider.readStatement() )
        {
            long propertyId = statement.propertyKeyGetForName( key );
            return statement.relationshipGetProperty( relId, propertyId ).value( defaultValue );
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
            return false;

        try ( ReadStatement statement = statementCtxProvider.readStatement() )
        {
            long propertyId = statement.propertyKeyGetForName( key );
            return propertyId != KeyReadOperations.NO_SUCH_PROPERTY_KEY &&
                   statement.relationshipGetProperty( relId, propertyId ).isDefined();
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
        try ( DataStatement statement = statementCtxProvider.dataStatement() )
        {
            long propertyKeyId = statement.propertyKeyGetOrCreateForName( key );
            statement.relationshipSetProperty( relId, Property.property( propertyKeyId, value ) );
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
        try ( DataStatement statement = statementCtxProvider.dataStatement() )
        {
            long propertyId = statement.propertyKeyGetOrCreateForName( key );
            return statement.relationshipRemoveProperty( relId, propertyId ).value( null );
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
        statementCtxProvider.assertInTransaction();
    }
}