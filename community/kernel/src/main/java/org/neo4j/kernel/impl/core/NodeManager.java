/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.lang.System.currentTimeMillis;

public class NodeManager extends LifecycleAdapter implements EntityFactory
{
    private final ThreadToStatementContextBridge threadToTransactionBridge;
    private final NodeProxy.NodeActions nodeActions;
    private final RelationshipProxy.RelationshipActions relationshipActions;
    private final GraphPropertiesProxy.GraphPropertiesActions graphPropertiesActions;

    private final List<PropertyTracker<Node>> nodePropertyTrackers;
    private final List<PropertyTracker<Relationship>> relationshipPropertyTrackers;
    private long epoch;
    private final GraphDatabaseService graphDatabaseService;
    private final RelationshipTypeTokenHolder relationshipTypeTokenHolder;

    public NodeManager( GraphDatabaseService graphDatabaseService,
                        ThreadToStatementContextBridge threadToTransactionBridge, RelationshipTypeTokenHolder
                        relationshipTypeTokenHolder )
    {
        this.graphDatabaseService = graphDatabaseService;
        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.nodeActions = new NodeActionsImpl();
        this.relationshipActions = new RelationshipActionsImpl();
        this.graphPropertiesActions = new GraphPropertiesActionsImpl();
        this.threadToTransactionBridge = threadToTransactionBridge;
        // Trackers may be added and removed at runtime, e.g. via the REST interface in server,
        // so we use the thread-safe CopyOnWriteArrayList.
        this.nodePropertyTrackers = new CopyOnWriteArrayList<>();
        this.relationshipPropertyTrackers = new CopyOnWriteArrayList<>();
    }

    @Override
    public void init()
    {   // Nothing to initialize
    }

    @Override
    public void start() throws Throwable
    {
        epoch = currentTimeMillis();
    }

    @Override
    public NodeProxy newNodeProxyById( long id )
    {
        return new NodeProxy( nodeActions, id );
    }

    /** Returns a "lazy" proxy, where additional fields are initialized on access. */
    @Override
    public RelationshipProxy newRelationshipProxyById( long id )
    {
        return new RelationshipProxy( relationshipActions, id );
    }

    /** Returns a fully initialized proxy. */
    public RelationshipProxy newRelationshipProxy( long id )
    {
        try ( Statement statement = threadToTransactionBridge.get() )
        {
            RelationshipProxy proxy = new RelationshipProxy( relationshipActions, id );
            statement.readOperations().relationshipVisit( id, proxy );
            return proxy;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
    }

    /** Returns a fully initialized proxy. */
    public RelationshipProxy newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId )
    {
        return new RelationshipProxy( relationshipActions, id, startNodeId, typeId, endNodeId );
    }

    @Override
    public GraphProperties newGraphProperties()
    {
        return new GraphPropertiesProxy( graphPropertiesActions );
    }

    public List<PropertyTracker<Node>> getNodePropertyTrackers()
    {
        return nodePropertyTrackers;
    }

    public List<PropertyTracker<Relationship>> getRelationshipPropertyTrackers()
    {
        return relationshipPropertyTrackers;
    }

    public void addNodePropertyTracker( PropertyTracker<Node> nodePropertyTracker )
    {
        nodePropertyTrackers.add( nodePropertyTracker );
    }

    public void removeNodePropertyTracker( PropertyTracker<Node> nodePropertyTracker )
    {
        nodePropertyTrackers.remove( nodePropertyTracker );
    }

    public void addRelationshipPropertyTracker( PropertyTracker<Relationship> relationshipPropertyTracker )
    {
        relationshipPropertyTrackers.add( relationshipPropertyTracker );
    }

    public void removeRelationshipPropertyTracker( PropertyTracker<Relationship> relationshipPropertyTracker )
    {
        relationshipPropertyTrackers.remove( relationshipPropertyTracker );
    }

    private class NodeActionsImpl implements NodeProxy.NodeActions
    {
        @Override
        public Statement statement()
        {
            return threadToTransactionBridge.get();
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            // TODO This should be wrapped as well
            return graphDatabaseService;
        }

        @Override
        public void assertInUnterminatedTransaction()
        {
            threadToTransactionBridge.assertInUnterminatedTransaction();
        }

        @Override
        public void failTransaction()
        {
            threadToTransactionBridge.getKernelTransactionBoundToThisThread( true ).failure();
        }

        @Override
        public Relationship lazyRelationshipProxy( long id )
        {
            return NodeManager.this.newRelationshipProxyById( id );
        }

        @Override
        public Relationship newRelationshipProxy( long id )
        {
            return NodeManager.this.newRelationshipProxy( id );
        }

        @Override
        public Relationship newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId )
        {
            return NodeManager.this.newRelationshipProxy( id, startNodeId, typeId, endNodeId );
        }
    }

    private class RelationshipActionsImpl implements RelationshipProxy.RelationshipActions
    {
        @Override
        public GraphDatabaseService getGraphDatabaseService()
        {
            return graphDatabaseService;
        }

        @Override
        public void failTransaction()
        {
            threadToTransactionBridge.getKernelTransactionBoundToThisThread( true ).failure();
        }

        @Override
        public void assertInUnterminatedTransaction()
        {
            threadToTransactionBridge.assertInUnterminatedTransaction();
        }

        @Override
        public Statement statement()
        {
            return threadToTransactionBridge.get();
        }

        @Override
        public Node newNodeProxy( long nodeId )
        {
            // only used by relationship already checked as valid in cache
            return NodeManager.this.newNodeProxyById( nodeId );
        }

        @Override
        public RelationshipType getRelationshipTypeById( int type )
        {
            try
            {
                return relationshipTypeTokenHolder.getTokenById( type );
            }
            catch ( TokenNotFoundException e )
            {
                throw new NotFoundException( e );
            }
        }
    };

    private class GraphPropertiesActionsImpl implements GraphPropertiesProxy.GraphPropertiesActions
    {
        @Override
        public GraphDatabaseService getGraphDatabaseService()
        {
            return graphDatabaseService;
        }

        @Override
        public void failTransaction()
        {
            threadToTransactionBridge.getKernelTransactionBoundToThisThread( true ).failure();
        }

        @Override
        public Statement statement()
        {
            return threadToTransactionBridge.get();
        }
    };
}
