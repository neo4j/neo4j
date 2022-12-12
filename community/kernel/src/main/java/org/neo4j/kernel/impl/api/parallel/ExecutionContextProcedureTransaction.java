/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api.parallel;

import java.util.Map;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.internal.helpers.collection.AbstractResourceIterable;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.impl.coreapi.DataLookup;
import org.neo4j.kernel.impl.coreapi.internal.CursorIterator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.ElementIdMapper;

public class ExecutionContextProcedureTransaction extends DataLookup implements Transaction {

    private final ExecutionContext executionContext;

    public ExecutionContextProcedureTransaction(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    @Override
    public Node createNode() {
        throw new UnsupportedOperationException("Write operations are unsupported during parallel execution");
    }

    @Override
    public Node createNode(Label... labels) {
        throw new UnsupportedOperationException("Write operations are unsupported during parallel execution");
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription() {
        throw new UnsupportedOperationException("Traversal operations are unsupported during parallel execution");
    }

    @Override
    public TraversalDescription traversalDescription() {
        throw new UnsupportedOperationException("Traversal operations are unsupported during parallel execution");
    }

    @Override
    public Result execute(String query) throws QueryExecutionException {
        throw new UnsupportedOperationException("Execution of other queries is unsupported during parallel execution");
    }

    @Override
    public Result execute(String query, Map<String, Object> parameters) throws QueryExecutionException {
        throw new UnsupportedOperationException("Execution of other queries is unsupported during parallel execution");
    }

    @Override
    public Iterable<Label> getAllLabelsInUse() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterable<Label> getAllLabels() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypes() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterable<String> getAllPropertyKeys() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ResourceIterable<Node> getAllNodes() {
        var result = new AbstractResourceIterable<Node>() {

            @Override
            protected ResourceIterator<Node> newIterator() {
                NodeCursor cursor = cursors().allocateNodeCursor(cursorContext());
                dataRead().allNodesScan(cursor);
                return new CursorIterator<>(cursor, NodeCursor::nodeReference, c -> newNodeEntity(c.nodeReference()));
            }

            @Override
            protected void onClosed() {
                executionContext.unregisterCloseableResource(this);
            }
        };

        executionContext.registerCloseableResource(result);
        return result;
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships() {
        var result = new AbstractResourceIterable<Relationship>() {

            @Override
            protected ResourceIterator<Relationship> newIterator() {
                RelationshipScanCursor cursor = cursors().allocateRelationshipScanCursor(cursorContext());
                dataRead().allRelationshipsScan(cursor);
                return new CursorIterator<>(
                        cursor,
                        RelationshipScanCursor::relationshipReference,
                        c -> newRelationshipEntity(c.relationshipReference()));
            }

            @Override
            protected void onClosed() {
                executionContext.unregisterCloseableResource(this);
            }
        };

        executionContext.registerCloseableResource(result);
        return result;
    }

    // Explicit locking could be easily supported, but it would give the users
    // tooling to create deadlocks within one transaction
    @Override
    public Lock acquireWriteLock(Entity entity) {
        throw new UnsupportedOperationException("Acquiring locks is unsupported during parallel execution.");
    }

    @Override
    public Lock acquireReadLock(Entity entity) {
        throw new UnsupportedOperationException("Acquiring locks is unsupported during parallel execution.");
    }

    // Schema operations could be easily supported, but the investment does not make sense since DDL is richer API.
    @Override
    public Schema schema() {
        throw new UnsupportedOperationException("Working with schema is unsupported during parallel execution.");
    }

    @Override
    public void terminate() {
        throw new UnsupportedOperationException(
                "Terminating ongoing transaction inside of a procedure or a function is unsupported.");
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException(
                "Committing ongoing transaction inside of a procedure or a function is unsupported.");
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException(
                "Rolling back ongoing transaction inside of a procedure or a function is unsupported.");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException(
                "Closing ongoing transaction inside of a procedure or a function is unsupported.");
    }

    @Override
    protected TokenRead tokenRead() {
        return executionContext.tokenRead();
    }

    @Override
    protected SchemaRead schemaRead() {
        return executionContext.schemaRead();
    }

    @Override
    protected Read dataRead() {
        return executionContext.dataRead();
    }

    @Override
    protected ResourceMonitor resourceMonitor() {
        return executionContext;
    }

    @Override
    protected Node newNodeEntity(long nodeId) {
        return new ExecutionContextNode(nodeId, executionContext);
    }

    @Override
    protected Relationship newRelationshipEntity(long relationshipId) {
        return new ExecutionContextRelationship(relationshipId, executionContext);
    }

    @Override
    protected Relationship newRelationshipEntity(long id, long startNodeId, int typeId, long endNodeId) {
        return newRelationshipEntity(id);
    }

    @Override
    protected CursorFactory cursors() {
        return executionContext.cursors();
    }

    @Override
    protected CursorContext cursorContext() {
        return executionContext.cursorContext();
    }

    @Override
    protected MemoryTracker memoryTracker() {
        return executionContext.memoryTracker();
    }

    @Override
    protected QueryContext queryContext() {
        return executionContext.queryContext();
    }

    @Override
    protected ElementIdMapper elementIdMapper() {
        return executionContext.elementIdMapper();
    }
}
