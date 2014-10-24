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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.kernel.impl.util.CappedOperation;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;

import static java.lang.System.currentTimeMillis;

public class NodeManager extends LifecycleAdapter implements EntityFactory
{
    private final ThreadToStatementContextBridge threadToTransactionBridge;
    private final NodeProxy.NodeLookup nodeLookup;
    private final RelationshipProxy.RelationshipLookups relationshipLookups;

    private final List<PropertyTracker<Node>> nodePropertyTrackers;
    private final List<PropertyTracker<Relationship>> relationshipPropertyTrackers;
    private final Logging logging;
    private long epoch;
    private final PropertyChainVerifier propertyChainVerifier;
    private GraphPropertiesImpl graphProperties;

    public NodeManager( NodeProxy.NodeLookup nodeLookup, RelationshipProxy.RelationshipLookups relationshipLookups,
                        ThreadToStatementContextBridge threadToTransactionBridge, Logging logging )
    {
        this.nodeLookup = nodeLookup;
        this.relationshipLookups = relationshipLookups;
        this.threadToTransactionBridge = threadToTransactionBridge;

        // Trackers may be added and removed at runtime, e.g. via the REST interface in server,
        // so we use the thread-safe CopyOnWriteArrayList.
        this.nodePropertyTrackers = new CopyOnWriteArrayList<>();
        this.relationshipPropertyTrackers = new CopyOnWriteArrayList<>();

        this.logging = logging;
        this.propertyChainVerifier = new NoDuplicatesPropertyChainVerifier();
    }

    @Override
    public void init()
    {   // Nothing to initialize
    }

    @Override
    public void start() throws Throwable
    {
        epoch = currentTimeMillis();
        propertyChainVerifier.addObserver( new CappedLoggingDuplicatePropertyObserver(
                new ConsoleLogger( StringLogger.cappedLogger(
                        logging.getMessagesLog( NoDuplicatesPropertyChainVerifier.class ),
                        CappedOperation.<String>time( 2, TimeUnit.HOURS ) ) )
        ) );
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

    @Override
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

    public PropertyChainVerifier getPropertyChainVerifier()
    {
        return propertyChainVerifier;
    }

    public static class CappedLoggingDuplicatePropertyObserver implements PropertyChainVerifier.Observer
    {
        public static final String DUPLICATE_WARNING_MESSAGE = "WARNING: Duplicate property records have been " +
                "detected in" +
                " this database store. For further details and resolution please refer to http://neo4j" +
                ".com/technote/cr73nh";

        private final ConsoleLogger consoleLogger;

        public CappedLoggingDuplicatePropertyObserver( ConsoleLogger consoleLogger )
        {
            this.consoleLogger = consoleLogger;
        }

        @Override
        public void inconsistencyFound( Primitive owningPrimitive )
        {
            consoleLogger.log( DUPLICATE_WARNING_MESSAGE );
        }
    }
}
