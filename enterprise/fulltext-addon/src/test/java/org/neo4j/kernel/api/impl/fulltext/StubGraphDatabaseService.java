/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;

public class StubGraphDatabaseService implements GraphDatabaseService
{
    @Override
    public Node createNode()
    {
        return null;
    }

    @Override
    public Long createNodeId()
    {
        return null;
    }

    @Override
    public Node createNode( Label... labels )
    {
        return null;
    }

    @Override
    public Node getNodeById( long id )
    {
        return null;
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        return null;
    }

    @Override
    public ResourceIterable<Node> getAllNodes()
    {
        return null;
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships()
    {
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key, Object value )
    {
        return null;
    }

    @Override
    public Node findNode( Label label, String key, Object value )
    {
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label )
    {
        return null;
    }

    @Override
    public ResourceIterable<Label> getAllLabelsInUse()
    {
        return null;
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypesInUse()
    {
        return null;
    }

    @Override
    public ResourceIterable<Label> getAllLabels()
    {
        return null;
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypes()
    {
        return null;
    }

    @Override
    public ResourceIterable<String> getAllPropertyKeys()
    {
        return null;
    }

    @Override
    public boolean isAvailable( long timeout )
    {
        return false;
    }

    @Override
    public void shutdown()
    {

    }

    @Override
    public Transaction beginTx()
    {
        return null;
    }

    @Override
    public Transaction beginTx( long timeout, TimeUnit unit )
    {
        return null;
    }

    @Override
    public Result execute( String query ) throws QueryExecutionException
    {
        return null;
    }

    @Override
    public Result execute( String query, long timeout, TimeUnit unit ) throws QueryExecutionException
    {
        return null;
    }

    @Override
    public Result execute( String query, Map<String,Object> parameters ) throws QueryExecutionException
    {
        return null;
    }

    @Override
    public Result execute( String query, Map<String,Object> parameters, long timeout, TimeUnit unit )
            throws QueryExecutionException
    {
        return null;
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler( TransactionEventHandler<T> handler )
    {
        return null;
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler( TransactionEventHandler<T> handler )
    {
        return null;
    }

    @Override
    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        return null;
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return null;
    }

    @Override
    public Schema schema()
    {
        return null;
    }

    @Override
    public IndexManager index()
    {
        return null;
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        return null;
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        return null;
    }
}
