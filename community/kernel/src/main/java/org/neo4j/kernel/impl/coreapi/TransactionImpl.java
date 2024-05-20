/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.coreapi;

import static java.util.Collections.emptyMap;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;
import static org.neo4j.kernel.impl.coreapi.DefaultTransactionExceptionMapper.mapStatusException;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.exceptions.CypherExecutionException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.collection.AbstractResourceIterable;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TokenCapacityExceededKernelException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.api.CloseableResourceManager;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.impl.coreapi.internal.CursorIterator;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.traversal.BidirectionalTraversalDescriptionImpl;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.virtual.MapValue;

/**
 * Default implementation of {@link org.neo4j.graphdb.Transaction}
 */
public class TransactionImpl extends DataLookup implements InternalTransaction {
    private final TokenHolders tokenHolders;
    private final TransactionalContextFactory contextFactory;
    private final DatabaseAvailabilityGuard availabilityGuard;
    private final QueryExecutionEngine executionEngine;
    private final Consumer<Status> terminationCallback;
    private final TransactionExceptionMapper exceptionMapper;
    private final ElementIdMapper elementIdMapper;
    /**
     * Tracker of resources in use by the Core API.
     * <p>
     * Some results returned by the Core API represent resources that need to be closed.
     * Unfortunately, users might forget to close such resources when they are done with them.
     * Transaction resources are tracked here and unclosed resources are closed
     * at the end of the transaction.
     * <p>
     * This resource tracker does not track resources opened as part of Cypher execution,
     * which are managed in {@link org.neo4j.kernel.impl.api.KernelStatement}.
     */
    private final ResourceTracker coreApiResourceTracker;

    private KernelTransaction transaction;
    private boolean closed;

    public TransactionImpl(
            TokenHolders tokenHolders,
            TransactionalContextFactory contextFactory,
            DatabaseAvailabilityGuard availabilityGuard,
            QueryExecutionEngine executionEngine,
            KernelTransaction transaction,
            ElementIdMapper elementIdMapper) {
        this(
                tokenHolders,
                contextFactory,
                availabilityGuard,
                executionEngine,
                transaction,
                new CloseableResourceManager(),
                null,
                null,
                elementIdMapper);
    }

    public TransactionImpl(
            TokenHolders tokenHolders,
            TransactionalContextFactory contextFactory,
            DatabaseAvailabilityGuard availabilityGuard,
            QueryExecutionEngine executionEngine,
            KernelTransaction transaction,
            ResourceTracker coreApiResourceTracker,
            Consumer<Status> terminationCallback,
            TransactionExceptionMapper exceptionMapper,
            ElementIdMapper elementIdMapper) {
        this.tokenHolders = tokenHolders;
        this.contextFactory = contextFactory;
        this.availabilityGuard = availabilityGuard;
        this.executionEngine = executionEngine;
        this.coreApiResourceTracker = coreApiResourceTracker;
        this.terminationCallback = terminationCallback;
        this.exceptionMapper = exceptionMapper;
        this.elementIdMapper = elementIdMapper;
        setTransaction(transaction);
    }

    @Override
    public void registerCloseableResource(AutoCloseable closeableResource) {
        coreApiResourceTracker.registerCloseableResource(closeableResource);
    }

    @Override
    public void unregisterCloseableResource(AutoCloseable closeableResource) {
        coreApiResourceTracker.unregisterCloseableResource(closeableResource);
    }

    @Override
    public void commit() {
        commit(KernelTransaction.NO_MONITOR);
    }

    @Override
    public void commit(KernelTransaction.KernelTransactionMonitor kernelTransactionMonitor) {
        safeTerminalOperation(transaction -> transaction.commit(kernelTransactionMonitor));
    }

    @Override
    public void rollback() {
        if (isOpen()) {
            safeTerminalOperation(KernelTransaction::rollback);
        }
    }

    @Override
    public Node createNode() {
        var ktx = kernelTransaction();
        try {
            return newNodeEntity(ktx.dataWrite().nodeCreate());
        } catch (InvalidTransactionTypeKernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

    @Override
    public Node createNode(Label... labels) {
        var ktx = kernelTransaction();
        int[] labelIds;
        try {
            labelIds = new int[labels.length];
            String[] labelNames = new String[labels.length];
            for (int i = 0; i < labelNames.length; i++) {
                labelNames[i] = labels[i].name();
            }
            ktx.tokenWrite().labelGetOrCreateForNames(labelNames, labelIds);
        } catch (IllegalTokenNameException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        } catch (TokenCapacityExceededKernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        } catch (KernelException e) {
            throw mapStatusException("Unknown error trying to create label token", e.status(), e);
        }

        try {
            long nodeId = ktx.dataWrite().nodeCreateWithLabels(labelIds);
            return newNodeEntity(nodeId);
        } catch (ConstraintValidationException e) {
            throw new ConstraintViolationException("Unable to add label.", e);
        } catch (InvalidTransactionTypeKernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

    @Override
    public Result execute(String query) throws QueryExecutionException {
        return execute(query, emptyMap());
    }

    @Override
    public Result execute(String query, Map<String, Object> parameters) throws QueryExecutionException {
        return execute(this, query, ValueUtils.asParameterMapValue(parameters));
    }

    private Result execute(InternalTransaction transaction, String query, MapValue parameters)
            throws QueryExecutionException {
        checkInTransaction();
        TransactionalContext context =
                contextFactory.newContext(transaction, query, parameters, QueryExecutionConfiguration.DEFAULT_CONFIG);
        try {
            availabilityGuard.assertDatabaseAvailable();
            return executionEngine.executeQuery(query, parameters, context, false);
        } catch (UnavailableException ue) {
            throw new org.neo4j.graphdb.TransactionFailureException(ue.getMessage(), ue, ue.status());
        } catch (QueryExecutionKernelException e) {
            throw e.asUserException();
        }
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription() {
        checkInTransaction();
        return new BidirectionalTraversalDescriptionImpl();
    }

    @Override
    public TraversalDescription traversalDescription() {
        checkInTransaction();
        return new MonoDirectionalTraversalDescription();
    }

    @Override
    public ResourceIterable<Node> getAllNodes() {
        checkInTransaction();
        return new TrackingResourceIterable<>(this, new NodesProvider());
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships() {
        checkInTransaction();
        return new TrackingResourceIterable<>(this, new RelationshipsProvider());
    }

    @Override
    public final void terminate() {
        terminate(Terminated);
    }

    @Override
    public void terminate(Status reason) {
        var ktx = transaction;
        if (ktx == null) {
            return;
        }
        ktx.markForTermination(reason);
        if (terminationCallback != null) {
            terminationCallback.accept(reason);
        }
    }

    @Override
    public UUID getDatabaseId() {
        if (this.transaction != null) {
            return this.transaction.getDatabaseId();
        } else {
            return null;
        }
    }

    @Override
    public String getDatabaseName() {
        if (this.transaction != null) {
            return this.transaction.getDatabaseName();
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        if (isOpen()) {
            safeTerminalOperation(tx -> {});
        }
    }

    /**
     * This method performs operation *and* closes transaction
     */
    private void safeTerminalOperation(TransactionalOperation operation) {
        if (closed) {
            assert transaction == null : "Closed but still have reference to kernel transaction";
            throw exceptionMapper.mapException(new NotInTransactionException("The transaction has been closed."));
        }
        Exception exception = null;
        try {
            try {
                coreApiResourceTracker.closeAllCloseableResources();
                operation.perform(transaction);
            } catch (Exception e) {
                exception = e;
            } finally {
                if (transaction != null) {
                    transaction.close();
                }
            }
        } catch (Exception e) {
            exception = Exceptions.chain(exception, e);
        } finally {
            closed = true;
            transaction = null;
        }
        if (exception != null) {
            throw exceptionMapper.mapException(exception);
        }
    }

    @Override
    public void setTransaction(KernelTransaction transaction) {
        this.transaction = transaction;
        transaction.bindToUserTransaction(this);
    }

    @Override
    public Lock acquireWriteLock(Entity entity) {
        return EntityLocker.exclusiveLock(kernelTransaction(), entity);
    }

    @Override
    public Lock acquireReadLock(Entity entity) {
        return EntityLocker.sharedLock(kernelTransaction(), entity);
    }

    @Override
    public KernelTransaction kernelTransaction() {
        checkInTransaction();
        return transaction;
    }

    @Override
    public KernelTransaction.Type transactionType() {
        return kernelTransaction().transactionType();
    }

    @Override
    public SecurityContext securityContext() {
        return kernelTransaction().securityContext();
    }

    @Override
    public ClientConnectionInfo clientInfo() {
        return kernelTransaction().clientInfo();
    }

    @Override
    public KernelTransaction.Revertable overrideWith(SecurityContext context) {
        return kernelTransaction().overrideWith(context);
    }

    @Override
    public Optional<Status> terminationReason() {
        var tx = transaction;
        return tx != null ? tx.getReasonIfTerminated() : Optional.empty();
    }

    @Override
    public void setMetaData(Map<String, Object> txMeta) {
        kernelTransaction().setMetaData(txMeta);
    }

    @Override
    public RelationshipEntity newRelationshipEntity(long id) {
        return new RelationshipEntity(this, id);
    }

    @Override
    public Relationship newRelationshipEntity(String elementId) {
        return new RelationshipEntity(this, elementIdMapper.relationshipId(elementId));
    }

    @Override
    public RelationshipEntity newRelationshipEntity(long id, long startNodeId, int typeId, long endNodeId) {
        return new RelationshipEntity(this, id, startNodeId, typeId, endNodeId);
    }

    @Override
    public Relationship newRelationshipEntity(RelationshipDataAccessor cursor) {
        return new RelationshipEntity(this, cursor);
    }

    @Override
    public NodeEntity newNodeEntity(long nodeId) {
        return new NodeEntity(this, nodeId);
    }

    @Override
    protected CursorFactory cursors() {
        return kernelTransaction().cursors();
    }

    @Override
    protected CursorContext cursorContext() {
        return kernelTransaction().cursorContext();
    }

    @Override
    protected MemoryTracker memoryTracker() {
        return kernelTransaction().memoryTracker();
    }

    @Override
    protected QueryContext queryContext() {
        return kernelTransaction().queryContext();
    }

    @Override
    public RelationshipType getRelationshipTypeById(int type) {
        try {
            String name =
                    tokenHolders.relationshipTypeTokens().getTokenById(type).name();
            return RelationshipType.withName(name);
        } catch (TokenNotFoundException e) {
            throw new IllegalStateException("Kernel API returned non-existent relationship type: " + type, e);
        }
    }

    @Override
    public Schema schema() {
        return new SchemaImpl(kernelTransaction());
    }

    @Override
    protected TokenRead tokenRead() {
        return kernelTransaction().tokenRead();
    }

    @Override
    protected SchemaRead schemaRead() {
        return kernelTransaction().schemaRead();
    }

    @Override
    protected Read dataRead() {
        return kernelTransaction().dataRead();
    }

    @Override
    protected ResourceMonitor resourceMonitor() {
        return coreApiResourceTracker;
    }

    @Override
    public Entity validateSameDB(Entity entity) {
        return validateSameDB(this, entity);
    }

    @Override
    public void checkInTransaction() {
        if (closed) {
            throw new NotInTransactionException("The transaction has been closed.");
        }
        if (transaction.isTerminated()) {
            Status terminationReason = transaction.getReasonIfTerminated().orElse(Status.Transaction.Terminated);
            throw new TransactionTerminatedException(terminationReason);
        }
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public ElementIdMapper elementIdMapper() {
        return elementIdMapper;
    }

    @Override
    protected void performCheckBeforeOperation() {
        checkInTransaction();
    }

    private static class NodesProvider implements Function<TransactionImpl, ResourceIterator<Node>> {
        @Override
        public ResourceIterator<Node> apply(TransactionImpl tx) {
            KernelTransaction ktx = tx.transaction;
            NodeCursor cursor = ktx.cursors().allocateNodeCursor(ktx.cursorContext(), ktx.memoryTracker());
            ktx.dataRead().allNodesScan(cursor);
            return new CursorIterator<>(cursor, NodeCursor::nodeReference, c -> tx.newNodeEntity(c.nodeReference()));
        }
    }

    public static Entity validateSameDB(InternalTransaction tx, Entity entity) {
        InternalTransaction internalTransaction;

        if (entity instanceof NodeEntity node) {
            internalTransaction = node.getTransaction();
        } else if (entity instanceof RelationshipEntity rel) {
            internalTransaction = rel.getTransaction();
        } else {
            return entity;
        }

        if (!internalTransaction.isOpen()) {
            throw new NotInTransactionException(
                    "The transaction of entity " + entity.getElementId() + " has been closed.");
        }

        if (internalTransaction.getDatabaseId() != tx.getDatabaseId()) {
            throw new CypherExecutionException("Can not use an entity from another database. Entity element id: "
                    + entity.getElementId() + ", entity database: "
                    + internalTransaction.getDatabaseName() + ", expected database: "
                    + tx.getDatabaseName() + ".");
        }
        return entity;
    }

    private static class RelationshipsProvider implements Function<TransactionImpl, ResourceIterator<Relationship>> {
        @Override
        public ResourceIterator<Relationship> apply(TransactionImpl tx) {
            KernelTransaction ktx = tx.transaction;
            RelationshipScanCursor cursor =
                    ktx.cursors().allocateRelationshipScanCursor(ktx.cursorContext(), ktx.memoryTracker());
            ktx.dataRead().allRelationshipsScan(cursor);
            return new CursorIterator<>(
                    cursor, RelationshipScanCursor::relationshipReference, c -> tx.newRelationshipEntity(cursor));
        }
    }

    private static class TrackingResourceIterable<T> extends AbstractResourceIterable<T> {
        private final TransactionImpl transaction;

        private final Function<TransactionImpl, ResourceIterator<T>> cursorProvider;

        private TrackingResourceIterable(
                TransactionImpl transaction, Function<TransactionImpl, ResourceIterator<T>> cursorProvider) {
            this.transaction = transaction;
            this.cursorProvider = cursorProvider;

            transaction.registerCloseableResource(this);
        }

        @Override
        protected ResourceIterator<T> newIterator() {
            return cursorProvider.apply(transaction);
        }

        @Override
        protected void onClosed() {
            transaction.unregisterCloseableResource(this);
        }
    }

    @FunctionalInterface
    private interface TransactionalOperation {
        void perform(KernelTransaction transaction) throws Exception;
    }
}
