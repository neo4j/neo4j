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

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.withResource;

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
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.traversal.OldTraverserWrapper;

public class NodeProxy implements Node
{
    public interface NodeLookup
    {
        NodeImpl lookup(long nodeId);
        GraphDatabaseService getGraphDatabase();
        NodeManager getNodeManager();
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
        nodeLookup.lookup(nodeId, LockType.WRITE).delete( nodeLookup.getNodeManager(), this );
    }

    @Override
    public Iterable<Relationship> getRelationships()
    {
        return nodeLookup.lookup(nodeId).getRelationships( nodeLookup.getNodeManager() );
    }

    @Override
    public boolean hasRelationship()
    {
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager() );
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction dir )
    {
        return nodeLookup.lookup(nodeId).getRelationships( nodeLookup.getNodeManager(), dir );
    }

    @Override
    public boolean hasRelationship( Direction dir )
    {
        return nodeLookup.lookup(nodeId).hasRelationship( nodeLookup.getNodeManager(), dir );
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        return nodeLookup.lookup(nodeId).getRelationships( nodeLookup.getNodeManager(), types );
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )
    {
        return nodeLookup.lookup(nodeId).getRelationships( nodeLookup.getNodeManager(), direction, types );
    }

    @Override
    public boolean hasRelationship( RelationshipType... types )
    {
        return nodeLookup.lookup(nodeId).hasRelationship( nodeLookup.getNodeManager(), types );
    }

    @Override
    public boolean hasRelationship( Direction direction, RelationshipType... types )
    {
        return nodeLookup.lookup(nodeId).hasRelationship( nodeLookup.getNodeManager(), direction, types );
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType type,
        Direction dir )
    {
        return nodeLookup.lookup(nodeId).getRelationships( nodeLookup.getNodeManager(), type, dir );
    }

    @Override
    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        return nodeLookup.lookup(nodeId).hasRelationship( nodeLookup.getNodeManager(), type, dir );
    }

    @Override
    public Relationship getSingleRelationship( RelationshipType type,
        Direction dir )
    {
        return nodeLookup.lookup(nodeId).getSingleRelationship( nodeLookup.getNodeManager(), type, dir );
    }

    @Override
    public void setProperty( String key, Object value )
    {
        nodeLookup.lookup(nodeId, LockType.WRITE).setProperty( nodeLookup.getNodeManager(), this, key, value );
    }

    @Override
    public Object removeProperty( String key ) throws NotFoundException
    {
        return nodeLookup.lookup(nodeId, LockType.WRITE).removeProperty( nodeLookup.getNodeManager(), this, key );
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        return nodeLookup.lookup(nodeId).getProperty( nodeLookup.getNodeManager(), key, defaultValue );
    }

    @Override
    public Iterable<Object> getPropertyValues()
    {
        return nodeLookup.lookup(nodeId).getPropertyValues( nodeLookup.getNodeManager() );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        return nodeLookup.lookup(nodeId).getPropertyKeys( nodeLookup.getNodeManager() );
    }

    @Override
    public Object getProperty( String key ) throws NotFoundException
    {
        return nodeLookup.lookup(nodeId).getProperty( nodeLookup.getNodeManager(), key );
    }

    @Override
    public boolean hasProperty( String key )
    {
        return nodeLookup.lookup(nodeId).hasProperty( nodeLookup.getNodeManager(), key );
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
        if ( !(o instanceof Node) )
    {
        return false;
    }
        return this.getId() == ((Node) o).getId();
    }

    @Override
    public int hashCode()
    {
        return (int) (( nodeId >>> 32 ) ^ nodeId );
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
        return nodeLookup.lookup(nodeId, LockType.WRITE).createRelationshipTo( nodeLookup.getNodeManager(), this, otherNode, type );
    }

    /* Tentative expansion API
    public Expansion<Relationship> expandAll()
    {
        return nodeLookup.lookup(nodeId).expandAll();
    }

    public Expansion<Relationship> expand( RelationshipType type )
    {
        return nodeLookup.lookup(nodeId).expand( type );
    }

    public Expansion<Relationship> expand( RelationshipType type,
            Direction direction )
    {
        return nodeLookup.lookup(nodeId).expand( type, direction );
    }

    public Expansion<Relationship> expand( Direction direction )
    {
        return nodeLookup.lookup(nodeId).expand( direction );
    }

    public Expansion<Relationship> expand( RelationshipExpander expander )
    {
        return nodeLookup.lookup(nodeId).expand( expander );
    }
    */

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
            ctx.addLabelToNode( ctx.getOrCreateLabelId( label.name() ), getId() );
        }
        catch ( ConstraintViolationKernelException e )
        {
            throw new ConstraintViolationException( "Unable to add label.", e );
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
        try
        {
            ctx.removeLabelFromNode( ctx.getLabelId( label.name() ), getId() );
        }
        catch ( LabelNotFoundKernelException e )
        {
            // OK, no such label... cool
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
            return ctx.isLabelSetOnNode( ctx.getLabelId( label.name() ), getId() );
        }
        catch ( LabelNotFoundKernelException e )
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
                return withResource( map(new Function<Long, Label>()
                {
                    @Override
                    public Label apply( Long labelId )
                    {
                        try
                        {
                            return label( ctx.getLabelName( labelId ) );
                        }
                        catch ( LabelNotFoundKernelException e )
                        {
                            throw new ThisShouldNotHappenError( "Mattias", "Listed labels for node " + nodeId +
                                    ", but the returned label " + labelId + " doesn't exist anymore" );
                        }
                    }
                },ctx.getLabelsForNode( getId() )), ctx );
            }
        };
    }
}
