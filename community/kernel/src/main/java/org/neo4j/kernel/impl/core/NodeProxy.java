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

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.FunctionFromPrimitiveLong;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.cleanup.CleanupService;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.traversal.OldTraverserWrapper;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class NodeProxy implements Node
{
    public interface NodeLookup
    {
        NodeImpl lookup( long nodeId );

        GraphDatabaseService getGraphDatabase();

        NodeManager getNodeManager();

        CleanupService getCleanupService();

        NodeImpl lookup( long nodeId, LockType lock );
    }

    private final NodeLookup nodeLookup;
    private final ThreadToStatementContextBridge statementCtxProvider;
    private final long nodeId;

    NodeProxy( long nodeId, NodeLookup nodeLookup, ThreadToStatementContextBridge statementCtxProvider )
    {
        this.nodeId = nodeId;
        this.nodeLookup = nodeLookup;
        this.statementCtxProvider = statementCtxProvider;
    }

    @Override
    public long getId()
    {
        return nodeId;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return nodeLookup.getGraphDatabase();
    }

    @Override
    public void delete()
    {
        StatementOperationParts context = statementCtxProvider.getCtxForWriting();
        StatementState state = statementCtxProvider.statementForWriting();
        try
        {
            context.entityWriteOperations().nodeDelete( state, getId() );
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public Iterable<Relationship> getRelationships()
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId ).getRelationships( nodeLookup.getNodeManager() );
    }

    @Override
    public boolean hasRelationship()
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager() );
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction dir )
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId ).getRelationships( nodeLookup.getNodeManager(), dir );
    }

    @Override
    public boolean hasRelationship( Direction dir )
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager(), dir );
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId ).getRelationships( nodeLookup.getNodeManager(), types );
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId ).getRelationships( nodeLookup.getNodeManager(), direction, types );
    }

    @Override
    public boolean hasRelationship( RelationshipType... types )
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager(), types );
    }

    @Override
    public boolean hasRelationship( Direction direction, RelationshipType... types )
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager(), direction, types );
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType type,
                                                    Direction dir )
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId ).getRelationships( nodeLookup.getNodeManager(), type, dir );
    }

    @Override
    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager(), type, dir );
    }

    @Override
    public Relationship getSingleRelationship( RelationshipType type,
                                               Direction dir )
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId ).getSingleRelationship( nodeLookup.getNodeManager(), type, dir );
    }

    private void assertInTransaction()
    {
        statementCtxProvider.assertInTransaction();
    }

    @Override
    public void setProperty( String key, Object value )
    {
        StatementOperationParts context = statementCtxProvider.getCtxForWriting();
        StatementState state = statementCtxProvider.statementForWriting();
        boolean success = false;
        try
        {
            long propertyKeyId = context.keyWriteOperations().propertyKeyGetOrCreateForName( state, key );
            context.entityWriteOperations().nodeSetProperty( state, nodeId, Property.property( propertyKeyId, value ) );
            success = true;
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Stefan/Jake", "A property key id disappeared under our feet" );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
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
                nodeLookup.getNodeManager().setRollbackOnly();
            }
        }
    }

    @Override
    public Object removeProperty( String key ) throws NotFoundException
    {
        StatementOperationParts context = statementCtxProvider.getCtxForWriting();
        StatementState state = statementCtxProvider.statementForWriting();
        try
        {
            long propertyKeyId = context.keyWriteOperations().propertyKeyGetOrCreateForName( state, key );
            return context.entityWriteOperations().nodeRemoveProperty( state, nodeId, propertyKeyId ).value( null );
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Stefan/Jake", "A property key id disappeared under our feet" );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
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
    public Object getProperty( String key, Object defaultValue )
    {
        // TODO: Push this check to getPropertyKeyId
        // ^^^^^ actually, if the key is null, we could fail before getting the statement context...
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        StatementOperationParts context = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
        try
        {
            long propertyKeyId = context.keyReadOperations().propertyKeyGetForName( state, key );
            return context.entityReadOperations().nodeGetProperty( state, nodeId, propertyKeyId ).value(defaultValue);
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
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
            }, context.entityReadOperations().nodeGetAllProperties( state, getId() )));
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found", e );
        }
        finally
        {
            state.close();
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
            PrimitiveLongIterator keyIds = context.entityReadOperations().nodeGetPropertyKeys( state, getId() );
            while ( keyIds.hasNext() )
            {
                keys.add( context.keyReadOperations().propertyKeyGetName( state, keyIds.next() ) );
            }
            return keys;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found", e );
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
    public Object getProperty( String key ) throws NotFoundException
    {
        // TODO: Push this check to getPropertyKeyId
        // ^^^^^ actually, if the key is null, we could fail before getting the statement context...
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        StatementOperationParts context = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
        try
        {
            long propertyKeyId = context.keyReadOperations().propertyKeyGetForName( state, key );
            return context.entityReadOperations().nodeGetProperty( state, nodeId, propertyKeyId ).value();
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
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
    public boolean hasProperty( String key )
    {
        if ( null == key )
            return false;

        StatementOperationParts context = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
        try
        {
            long propertyKeyId = context.keyReadOperations().propertyKeyGetForName( state, key );
            return context.entityReadOperations().nodeHasProperty( state, nodeId, propertyKeyId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
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

    public int compareTo( Object node )
    {
        Node n = (Node) node;
        long ourId = this.getId(), theirId = n.getId();

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
        return o instanceof Node && this.getId() == ((Node) o).getId();
    }

    @Override
    public int hashCode()
    {
        return (int) ((nodeId >>> 32) ^ nodeId);
    }

    @Override
    public String toString()
    {
        return "Node[" + this.getId() + "]";
    }

    @Override
    public Relationship createRelationshipTo( Node otherNode,
                                              RelationshipType type )
    {
        assertInTransaction();
        return nodeLookup.lookup( nodeId, LockType.WRITE ).createRelationshipTo( nodeLookup.getNodeManager(), this,
                otherNode, type );
    }

    @Override
    public Traverser traverse( Order traversalOrder,
                               StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
                               RelationshipType relationshipType, Direction direction )
    {
        assertInTransaction();
        return OldTraverserWrapper.traverse( this,
                traversalOrder, stopEvaluator,
                returnableEvaluator, relationshipType, direction );
    }

    @Override
    public Traverser traverse( Order traversalOrder,
                               StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
                               RelationshipType firstRelationshipType, Direction firstDirection,
                               RelationshipType secondRelationshipType, Direction secondDirection )
    {
        assertInTransaction();
        return OldTraverserWrapper.traverse( this,
                traversalOrder, stopEvaluator,
                returnableEvaluator, firstRelationshipType, firstDirection,
                secondRelationshipType, secondDirection );
    }

    @Override
    public Traverser traverse( Order traversalOrder,
                               StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
                               Object... relationshipTypesAndDirections )
    {
        assertInTransaction();
        return OldTraverserWrapper.traverse( this,
                traversalOrder, stopEvaluator,
                returnableEvaluator, relationshipTypesAndDirections );
    }

    @Override
    public void addLabel( Label label )
    {
        StatementOperationParts context = statementCtxProvider.getCtxForWriting();
        StatementState state = statementCtxProvider.statementForWriting();
        try
        {
            context.entityWriteOperations().nodeAddLabel( state, getId(),
                    context.keyWriteOperations().labelGetOrCreateForName( state, label.name() ) );
        }
        catch ( SchemaKernelException e )
        {
            throw new ConstraintViolationException( "Unable to add label.", e );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "No node with id " + getId() + " found.", e );
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public void removeLabel( Label label )
    {
        StatementOperationParts context = statementCtxProvider.getCtxForWriting();
        StatementState state = statementCtxProvider.statementForWriting();
        try
        {
            long labelId = context.keyReadOperations().labelGetForName( state, label.name() );
            context.entityWriteOperations().nodeRemoveLabel( state, getId(), labelId );
        }
        catch ( LabelNotFoundKernelException e )
        {
            return;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "No node with id " + getId() + " found.", e );
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public boolean hasLabel( Label label )
    {
        StatementOperationParts context = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
        try
        {
            return context.entityReadOperations().nodeHasLabel( state, getId(), context.keyReadOperations().labelGetForName( state, label.name() ) );
        }
        catch ( LabelNotFoundKernelException e )
        {
            return false;
        }
        catch ( EntityNotFoundException e )
        {
            return false;
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public ResourceIterable<Label> getLabels()
    {
        return new ResourceIterable<Label>()
        {
            @Override
            public ResourceIterator<Label> iterator()
            {
                final StatementOperationParts context = statementCtxProvider.getCtxForReading();
                PrimitiveLongIterator labels;

                final StatementState state = statementCtxProvider.statementForReading();
                try
                {
                    labels = context.entityReadOperations().nodeGetLabels( state, getId() );
                }
                catch ( EntityNotFoundException e )
                {
                    state.close();
                    throw new NotFoundException( "No node with id " + getId() + " found.", e );
                }

                return nodeLookup.getCleanupService().resourceIterator( map( new FunctionFromPrimitiveLong<Label>()
                {
                    @Override
                    public Label apply( long labelId )
                    {
                        try
                        {
                            return label( context.keyReadOperations().labelGetName( state, labelId ) );
                        }
                        catch ( LabelNotFoundKernelException e )
                        {
                            throw new ThisShouldNotHappenError( "Mattias", "Listed labels for node " + nodeId +
                                    ", but the returned label " + labelId + " doesn't exist anymore" );
                        }
                    }
                }, labels ), state );
            }
        };
    }
}
