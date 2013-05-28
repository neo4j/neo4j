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

import java.util.Iterator;

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
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.properties.Property;
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
        StatementContext ctxForWriting = statementCtxProvider.getCtxForWriting();
        try
        {
            ctxForWriting.nodeDelete( getId() );
        }
        finally
        {
            ctxForWriting.close();
        }
    }

    @Override
    public Iterable<Relationship> getRelationships()
    {
        return nodeLookup.lookup( nodeId ).getRelationships( nodeLookup.getNodeManager() );
    }

    @Override
    public boolean hasRelationship()
    {
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager() );
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction dir )
    {
        return nodeLookup.lookup( nodeId ).getRelationships( nodeLookup.getNodeManager(), dir );
    }

    @Override
    public boolean hasRelationship( Direction dir )
    {
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager(), dir );
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        return nodeLookup.lookup( nodeId ).getRelationships( nodeLookup.getNodeManager(), types );
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )
    {
        return nodeLookup.lookup( nodeId ).getRelationships( nodeLookup.getNodeManager(), direction, types );
    }

    @Override
    public boolean hasRelationship( RelationshipType... types )
    {
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager(), types );
    }

    @Override
    public boolean hasRelationship( Direction direction, RelationshipType... types )
    {
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager(), direction, types );
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType type,
                                                    Direction dir )
    {
        return nodeLookup.lookup( nodeId ).getRelationships( nodeLookup.getNodeManager(), type, dir );
    }

    @Override
    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager(), type, dir );
    }

    @Override
    public Relationship getSingleRelationship( RelationshipType type,
                                               Direction dir )
    {
        return nodeLookup.lookup( nodeId ).getSingleRelationship( nodeLookup.getNodeManager(), type, dir );
    }

    @Override
    public void setProperty( String key, Object value )
    {
        StatementContext ctxForWriting = statementCtxProvider.getCtxForWriting();
        boolean success = false;
        try
        {
            long propertyKeyId = ctxForWriting.propertyKeyGetOrCreateForName( key );
            ctxForWriting.nodeSetProperty( nodeId, Property.property( propertyKeyId, value ) );
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
            ctxForWriting.close();
            if ( !success )
            {
                nodeLookup.getNodeManager().setRollbackOnly();
            }
        }
    }

    @Override
    public Object removeProperty( String key ) throws NotFoundException
    {
        StatementContext ctxForWriting = statementCtxProvider.getCtxForWriting();
        try
        {
            long propertyId = ctxForWriting.propertyKeyGetOrCreateForName( key );
            return ctxForWriting.nodeRemoveProperty( nodeId, propertyId ).value( null );
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
            ctxForWriting.close();
        }
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        // TODO: Push this check to getPropertyKeyId
        // ^^^^^ actually, if the key is null, we could fail before getting the statement context...
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        StatementContext ctxForReading = statementCtxProvider.getCtxForReading();
        try
        {
            long propertyId = ctxForReading.propertyKeyGetForName( key );
            return ctxForReading.nodeGetProperty( nodeId, propertyId ).value(defaultValue);
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
            ctxForReading.close();
        }
    }

    @Override
    public Iterable<Object> getPropertyValues()
    {
        final StatementContext context = statementCtxProvider.getCtxForReading();
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
            }, context.nodeGetAllProperties( getId() )));
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found", e );
        }
        finally
        {
            context.close();
        }
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        final StatementContext context = statementCtxProvider.getCtxForReading();
        try
        {
            return asSet( map( new Function<Long, String>() {
                @Override
                public String apply( Long aLong )
                {
                    try
                    {
                        return context.propertyKeyGetName( aLong );
                    }
                    catch ( PropertyKeyIdNotFoundException e )
                    {
                        throw new ThisShouldNotHappenError( "Jake",
                                "Property key retrieved through kernel API should exist." );
                    }
                }
            }, context.nodeGetPropertyKeys( getId() )));
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found", e );
        }
        finally
        {
            context.close();
        }
    }

    @Override
    public Object getProperty( String key ) throws NotFoundException
    {
        // TODO: Push this check to getPropertyKeyId
        // ^^^^^ actually, if the key is null, we could fail before getting the statement context...
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        StatementContext ctxForReading = statementCtxProvider.getCtxForReading();
        try
        {
            long propertyId = ctxForReading.propertyKeyGetForName( key );
            return ctxForReading.nodeGetProperty( nodeId, propertyId ).value();
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
            ctxForReading.close();
        }
    }

    @Override
    public boolean hasProperty( String key )
    {
        if ( null == key )
            return false;

        StatementContext ctxForReading = statementCtxProvider.getCtxForReading();
        try
        {
            long propertyId = ctxForReading.propertyKeyGetForName( key );
            return ctxForReading.nodeHasProperty( nodeId, propertyId );
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
            ctxForReading.close();
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
        return nodeLookup.lookup( nodeId, LockType.WRITE ).createRelationshipTo( nodeLookup.getNodeManager(), this,
                otherNode, type );
    }

    @Override
    public Traverser traverse( Order traversalOrder,
                               StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
                               RelationshipType relationshipType, Direction direction )
    {
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
        return OldTraverserWrapper.traverse( this,
                traversalOrder, stopEvaluator,
                returnableEvaluator, relationshipTypesAndDirections );
    }

    @Override
    public void addLabel( Label label )
    {
        StatementContext ctx = statementCtxProvider.getCtxForWriting();
        try
        {
            ctx.nodeAddLabel( getId(), ctx.labelGetOrCreateForName( label.name() ) );
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
            ctx.close();
        }
    }

    @Override
    public void removeLabel( Label label )
    {
        StatementContext ctx = statementCtxProvider.getCtxForWriting();
        try {
            long labelId;
            try
            {
                labelId = ctx.labelGetForName( label.name() );
            }
            catch ( LabelNotFoundKernelException e )
            {
                return;
            }
            try
            {
                ctx.nodeRemoveLabel( getId(), labelId );
            }
            catch ( EntityNotFoundException e )
            {
                throw new NotFoundException( "No node with id " + getId() + " found.", e );
            }
        }
        finally
        {
            ctx.close();
        }
    }

    @Override
    public boolean hasLabel( Label label )
    {
        StatementContext ctx = statementCtxProvider.getCtxForReading();
        try
        {
            return ctx.nodeHasLabel( getId(), ctx.labelGetForName( label.name() ) );
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
            ctx.close();
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
                final StatementContext ctx = statementCtxProvider.getCtxForReading();
                Iterator<Long> labels;

                try
                {
                    labels = ctx.nodeGetLabels( getId() );
                }
                catch ( EntityNotFoundException e )
                {
                    ctx.close();
                    throw new NotFoundException( "No node with id " + getId() + " found.", e );
                }

                return nodeLookup.getCleanupService().resourceIterator( map( new Function<Long, Label>()
                {
                    @Override
                    public Label apply( Long labelId )
                    {
                        try
                        {
                            return label( ctx.labelGetName( labelId ) );
                        }
                        catch ( LabelNotFoundKernelException e )
                        {
                            throw new ThisShouldNotHappenError( "Mattias", "Listed labels for node " + nodeId +
                                    ", but the returned label " + labelId + " doesn't exist anymore" );
                        }
                    }
                }, labels ), ctx );
            }
        };
    }
}
