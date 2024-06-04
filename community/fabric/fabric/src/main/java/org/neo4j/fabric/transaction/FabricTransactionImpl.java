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
package org.neo4j.fabric.transaction;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.eval.Catalog;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.executor.Exceptions;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.FabricKernelTransaction;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.SingleDbTransaction;
import org.neo4j.fabric.planning.StatementType;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.transaction.parent.AbstractCompoundTransaction;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.transaction.trace.TraceProvider;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.time.SystemNanoClock;
import reactor.core.publisher.Mono;

public class FabricTransactionImpl extends AbstractCompoundTransaction<SingleDbTransaction>
        implements FabricTransaction, FabricTransaction.FabricExecutionContext {
    private final FabricTransactionInfo transactionInfo;
    private final TransactionBookmarkManager bookmarkManager;
    private final Catalog catalogSnapshot;
    private final TransactionManager transactionManager;
    private final FabricRemoteExecutor.RemoteTransactionContext remoteTransactionContext;
    private final FabricLocalExecutor.LocalTransactionContext localTransactionContext;
    private final AtomicReference<StatementType> statementType = new AtomicReference<>();

    private final LocationCache locationCache;

    private final TransactionInitializationTrace initializationTrace;

    private final FabricKernelTransaction kernelTransaction;

    private final Procedures contextlessProcedures;

    FabricTransactionImpl(
            FabricTransactionInfo transactionInfo,
            TransactionBookmarkManager bookmarkManager,
            FabricRemoteExecutor remoteExecutor,
            FabricLocalExecutor localExecutor,
            FabricProcedures contextlessProcedures,
            ErrorReporter errorReporter,
            TransactionManager transactionManager,
            Catalog catalogSnapshot,
            CatalogManager catalogManager,
            Boolean inCompositeContext,
            SystemNanoClock clock,
            TraceProvider traceProvider) {
        super(errorReporter, clock);

        this.transactionInfo = transactionInfo;
        this.transactionManager = transactionManager;
        this.bookmarkManager = bookmarkManager;
        this.catalogSnapshot = catalogSnapshot;
        this.initializationTrace = traceProvider.getTraceInfo();
        this.contextlessProcedures = contextlessProcedures;

        this.locationCache = new LocationCache(catalogManager, transactionInfo);

        try {
            remoteTransactionContext = remoteExecutor.startTransactionContext(this, transactionInfo, bookmarkManager);
            localTransactionContext = localExecutor.startTransactionContext(this, transactionInfo, bookmarkManager);
            DatabaseReference sessionDatabaseReference = getSessionDatabaseReference();
            if (inCompositeContext) {
                var graph = catalogSnapshot.resolveGraphByNameString(
                        sessionDatabaseReference.alias().name());
                var location = this.locationOf(graph, false);
                kernelTransaction = localTransactionContext.getOrCreateTx(
                        (Location.Local) location, TransactionMode.DEFINITELY_READ, true);
            } else {
                kernelTransaction = null;
            }
        } catch (RuntimeException e) {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
            throw Exceptions.transform(Status.Transaction.TransactionStartFailed, e);
        }
    }

    @Override
    public Catalog getCatalogSnapshot() {
        return catalogSnapshot;
    }

    @Override
    public FabricTransactionInfo getTransactionInfo() {
        return transactionInfo;
    }

    @Override
    public FabricRemoteExecutor.RemoteTransactionContext getRemote() {
        return remoteTransactionContext;
    }

    @Override
    public FabricLocalExecutor.LocalTransactionContext getLocal() {
        return localTransactionContext;
    }

    @Override
    public void validateStatementType(StatementType type) {
        boolean wasNull = statementType.compareAndSet(null, type);
        if (!wasNull) {
            var oldType = statementType.get();
            if (oldType != type) {
                var queryAfterQuery = type.isQuery() && oldType.isQuery();
                var readQueryAfterSchema = type.isReadQuery() && oldType.isSchemaCommand();
                var schemaAfterReadQuery = type.isSchemaCommand() && oldType.isReadQuery();
                var allowedCombination = queryAfterQuery || readQueryAfterSchema || schemaAfterReadQuery;
                if (allowedCombination) {
                    var writeQueryAfterReadQuery = queryAfterQuery && !type.isReadQuery() && oldType.isReadQuery();
                    var upgrade = writeQueryAfterReadQuery || schemaAfterReadQuery;
                    if (upgrade) {
                        statementType.set(type);
                    }
                } else {
                    throw new FabricException(
                            Status.Transaction.ForbiddenDueToTransactionType,
                            "Tried to execute %s after executing %s",
                            type,
                            oldType);
                }
            }
        }
    }

    public boolean isSchemaTransaction() {
        var type = statementType.get();
        return type != null && type.isSchemaCommand();
    }

    @Override
    public DatabaseReference getSessionDatabaseReference() {
        return transactionInfo.getSessionDatabaseReference();
    }

    @Override
    public Location locationOf(Catalog.Graph graph, Boolean requireWritable) {
        return locationCache.locationOf(graph, requireWritable);
    }

    @Override
    protected boolean isUninitialized() {
        return remoteTransactionContext == null && localTransactionContext == null;
    }

    @Override
    protected Mono<Void> childTransactionCommit(SingleDbTransaction singleDbTransaction) {
        return singleDbTransaction.commit();
    }

    @Override
    protected Mono<Void> childTransactionRollback(SingleDbTransaction singleDbTransaction) {
        return singleDbTransaction.rollback();
    }

    @Override
    protected Mono<Void> childTransactionTerminate(SingleDbTransaction singleDbTransaction, Status reason) {
        return singleDbTransaction.terminate(reason);
    }

    @Override
    protected void closeContextsAndRemoveTransaction() {
        remoteTransactionContext.close();
        localTransactionContext.close();
        transactionManager.removeTransaction(this);
    }

    @Override
    public StatementResult execute(Function<FabricExecutionContext, StatementResult> runLogic) {
        checkTransactionOpenForStatementExecution();

        try {
            return runLogic.apply(this);
        } catch (RuntimeException e) {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
            RuntimeException transformed = Exceptions.transform(Status.Statement.ExecutionFailed, e);
            try {
                rollback();
            } catch (Exception rollbackException) {
                transformed.addSuppressed(rollbackException);
            }
            throw transformed;
        }
    }

    public boolean isLocal() {
        return remoteTransactionContext.isEmptyContext();
    }

    @Override
    public TransactionBookmarkManager getBookmarkManager() {
        return bookmarkManager;
    }

    @Override
    public void setMetaData(Map<String, Object> txMeta) {
        transactionInfo.setMetaData(txMeta);
        for (InternalTransaction internalTransaction : getInternalTransactions()) {
            internalTransaction.setMetaData(txMeta);
        }
    }

    @Override
    public ExecutingQuery.TransactionBinding transactionBinding() throws FabricException {
        if (kernelTransaction == null) {
            return null;
        }
        DatabaseReference sessionDatabaseReference = getSessionDatabaseReference();
        NamedDatabaseId namedDbId =
                DatabaseIdFactory.from(sessionDatabaseReference.alias().name(), sessionDatabaseReference.id());

        long transactionSequenceNumber = kernelTransaction.transactionSequenceNumber();
        return new ExecutingQuery.TransactionBinding(
                namedDbId, () -> 0L, () -> 0L, () -> 0L, transactionSequenceNumber);
    }

    @Override
    public Procedures contextlessProcedures() {
        return contextlessProcedures;
    }

    @Override
    public CancellationChecker cancellationChecker() {
        return this::checkTransactionOpenForStatementExecution;
    }

    public TransactionInitializationTrace getInitializationTrace() {
        return initializationTrace;
    }

    public Set<InternalTransaction> getInternalTransactions() {
        return localTransactionContext.getInternalTransactions();
    }

    @Override
    public void closeTransaction(SingleDbTransaction databaseTransaction) {
        // only used in query router
    }
}
