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
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

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
        StatementOperationParts ctxForWriting = statementCtxProvider.getCtxForWriting();
        StatementState state = statementCtxProvider.statementForWriting();
        try
        {
            ctxForWriting.entityWriteOperations().relationshipDelete( state, getId() );
        }
        finally
        {
            state.close();
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
        final StatementOperationParts context = statementCtxProvider.getCtxForReading();
        final StatementState state = statementCtxProvider.statementForReading();
        try
        {
            List<String> keys = new ArrayList<>();
            PrimitiveLongIterator keyIds = context.entityReadOperations().relationshipGetPropertyKeys( state, getId() );
            while ( keyIds.hasNext() )
            {
                keys.add( context.keyReadOperations().propertyKeyGetName( state, keyIds.next() ) );
            }
            return keys;

        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Relationship not found", e );
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Jake",
                    "Property key retrieved through kernel API should exist." );
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public Iterable<Object> getPropertyValues()
    {
        final StatementOperationParts context = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
        try
        {
            return asSet( map( new Function<Property,Object>() {
                @Override
                public Object apply( Property prop )
                {
                    try
                    {
                        return prop.value();
                    }
                    catch ( PropertyNotFoundException e )
                    {
                        throw new ThisShouldNotHappenError( "Jake",
                                "Property key retrieved through kernel API should exist." );
                    }
                }
            }, context.entityReadOperations().relationshipGetAllProperties( state, getId() )));
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Relationship has been deleted", e );
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public Object getProperty( String key )
    {
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        StatementOperationParts ctxForReading = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
        try
        {
            long propertyId = ctxForReading.keyReadOperations().propertyKeyGetForName( state, key );
            return ctxForReading.entityReadOperations().relationshipGetProperty( state, relId, propertyId ).value();
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( PropertyKeyNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( PropertyNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        // TODO: Push this check to getPropertyKeyId
        // ^^^^^ actually, if the key is null, we could fail before getting the statement context...
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        StatementOperationParts ctxForReading = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
        try
        {
            long propertyId = ctxForReading.keyReadOperations().propertyKeyGetForName( state, key );
            return ctxForReading.entityReadOperations().relationshipGetProperty( state, relId, propertyId ).value(defaultValue);
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            return defaultValue;
        }
        catch ( PropertyKeyNotFoundException e )
        {
            return defaultValue;
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public boolean hasProperty( String key )
    {
        if ( null == key )
            return false;

        StatementOperationParts ctxForReading = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
        try
        {
            long propertyId = ctxForReading.keyReadOperations().propertyKeyGetForName( state, key );
            return ctxForReading.entityReadOperations().relationshipHasProperty( state, relId, propertyId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            return false;
        }
        catch ( PropertyKeyNotFoundException e )
        {
            return false;
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public void setProperty( String key, Object value )
    {
        StatementOperationParts ctxForWriting = statementCtxProvider.getCtxForWriting();
        StatementState state = statementCtxProvider.statementForWriting();
        boolean success = false;
        try
        {
            long propertyKeyId = ctxForWriting.keyWriteOperations().propertyKeyGetOrCreateForName( state, key );
            ctxForWriting.entityWriteOperations().relationshipSetProperty( state, relId, Property.property( propertyKeyId, value ) );
            success = true;
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Stefan/Jake", "A property key id disappeared under our feet" );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( SchemaKernelException e )
        {
            // TODO: Maybe throw more context-specific error than just IllegalArgument
            throw new IllegalArgumentException( e );
        }
        finally
        {
            state.close();
            if ( !success )
            {
                relationshipLookups.getNodeManager().setRollbackOnly();
            }
        }
    }

    @Override
    public Object removeProperty( String key )
    {
        StatementOperationParts ctxForWriting = statementCtxProvider.getCtxForWriting();
        StatementState state = statementCtxProvider.statementForWriting();
        try
        {
            long propertyId = ctxForWriting.keyWriteOperations().propertyKeyGetOrCreateForName( state, key );
            return ctxForWriting.entityWriteOperations().relationshipRemoveProperty( state, relId, propertyId ).value( null );
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Stefan/Jake", "A property key id disappeared under our feet" );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( SchemaKernelException e )
        {
            // TODO: Maybe throw more context-specific error than just IllegalArgument
            throw new IllegalArgumentException( e );
        }
        finally
        {
            state.close();
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