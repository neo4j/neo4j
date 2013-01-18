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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.LabelNotFoundException;
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

    public long getId()
    {
        return nodeId;
    }

    public GraphDatabaseService getGraphDatabase()
    {
        return nodeLookup.getGraphDatabase();
    }

    public void delete()
    {
        nodeLookup.lookup(nodeId, LockType.WRITE).delete( nodeLookup.getNodeManager(), this );
    }

    public Iterable<Relationship> getRelationships()
    {
        return nodeLookup.lookup(nodeId).getRelationships( nodeLookup.getNodeManager() );
    }

    public boolean hasRelationship()
    {
        return nodeLookup.lookup( nodeId ).hasRelationship( nodeLookup.getNodeManager() );
    }

    public Iterable<Relationship> getRelationships( Direction dir )
    {
        return nodeLookup.lookup(nodeId).getRelationships( nodeLookup.getNodeManager(), dir );
    }

    public boolean hasRelationship( Direction dir )
    {
        return nodeLookup.lookup(nodeId).hasRelationship( nodeLookup.getNodeManager(), dir );
    }

    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        return nodeLookup.lookup(nodeId).getRelationships( nodeLookup.getNodeManager(), types );
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )
    {
        return nodeLookup.lookup(nodeId).getRelationships( nodeLookup.getNodeManager(), direction, types );
    }

    public boolean hasRelationship( RelationshipType... types )
    {
        return nodeLookup.lookup(nodeId).hasRelationship( nodeLookup.getNodeManager(), types );
    }

    public boolean hasRelationship( Direction direction, RelationshipType... types )
    {
        return nodeLookup.lookup(nodeId).hasRelationship( nodeLookup.getNodeManager(), direction, types );
    }

    public Iterable<Relationship> getRelationships( RelationshipType type,
        Direction dir )
    {
        return nodeLookup.lookup(nodeId).getRelationships( nodeLookup.getNodeManager(), type, dir );
    }

    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        return nodeLookup.lookup(nodeId).hasRelationship( nodeLookup.getNodeManager(), type, dir );
    }

    public Relationship getSingleRelationship( RelationshipType type,
        Direction dir )
    {
        return nodeLookup.lookup(nodeId).getSingleRelationship( nodeLookup.getNodeManager(), type, dir );
    }

    public void setProperty( String key, Object value )
    {
        nodeLookup.lookup(nodeId, LockType.WRITE).setProperty( nodeLookup.getNodeManager(), this, key, value );
    }

    public Object removeProperty( String key ) throws NotFoundException
    {
        return nodeLookup.lookup(nodeId, LockType.WRITE).removeProperty( nodeLookup.getNodeManager(), this, key );
    }

    public Object getProperty( String key, Object defaultValue )
    {
        return nodeLookup.lookup(nodeId).getProperty( nodeLookup.getNodeManager(), key, defaultValue );
    }

    public Iterable<Object> getPropertyValues()
    {
        return nodeLookup.lookup(nodeId).getPropertyValues( nodeLookup.getNodeManager() );
    }

    public Iterable<String> getPropertyKeys()
    {
        return nodeLookup.lookup(nodeId).getPropertyKeys( nodeLookup.getNodeManager() );
    }

    public Object getProperty( String key ) throws NotFoundException
    {
        return nodeLookup.lookup(nodeId).getProperty( nodeLookup.getNodeManager(), key );
    }

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

    public Traverser traverse( Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RelationshipType relationshipType, Direction direction )
    {
        return OldTraverserWrapper.traverse( this,
                                             traversalOrder, stopEvaluator,
                                             returnableEvaluator, relationshipType, direction );
    }

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
        ctx.addLabelToNode( ctx.getOrCreateLabelId( label.name() ), getId() );
    }

    @Override
    public boolean hasLabel( Label label )
    {
        try
        {
            StatementContext ctx = statementCtxProvider.getCtxForReading();
            return ctx.isLabelSetOnNode( ctx.getLabelId( label.name() ), getId() );
        }
        catch ( LabelNotFoundException e )
        {
            return false;
        }
    }
}
