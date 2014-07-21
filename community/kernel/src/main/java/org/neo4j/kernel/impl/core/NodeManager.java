/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.lang.System.currentTimeMillis;

public class NodeManager extends LifecycleAdapter implements EntityFactory
{
    private final ThreadToStatementContextBridge threadToTransactionBridge;
    private final NodeProxy.NodeLookup nodeLookup;
    private final RelationshipProxy.RelationshipLookups relationshipLookups;
    private final List<PropertyTracker<Node>> nodePropertyTrackers;
    private final List<PropertyTracker<Relationship>> relationshipPropertyTrackers;
    private long epoch;

    public NodeManager( NodeProxy.NodeLookup nodeLookup, RelationshipProxy.RelationshipLookups relationshipLookups,
                        ThreadToStatementContextBridge threadToTransactionBridge )
    {
        this.nodeLookup = nodeLookup;
        this.relationshipLookups = relationshipLookups;
        this.threadToTransactionBridge = threadToTransactionBridge;
        this.nodePropertyTrackers = new LinkedList<>();
        this.relationshipPropertyTrackers = new LinkedList<>();
    }

    @Override
    public void start() throws Throwable
    {
        epoch = currentTimeMillis();
    }

    @Override
    public NodeProxy newNodeProxyById( long id )
    {
        return new NodeProxy( id, nodeLookup, relationshipLookups, threadToTransactionBridge );
    }

    @Override
    public RelationshipProxy newRelationshipProxyById( long id )
    {
        return new RelationshipProxy( id, relationshipLookups, threadToTransactionBridge );
    }

    public GraphPropertiesImpl newGraphProperties()
    {
        return new GraphPropertiesImpl( epoch, threadToTransactionBridge );
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
}
