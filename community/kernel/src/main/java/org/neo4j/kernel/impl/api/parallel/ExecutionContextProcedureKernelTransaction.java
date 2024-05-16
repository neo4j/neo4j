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
package org.neo4j.kernel.impl.api.parallel;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Upgrade;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.InnerTransactionHandler;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.TransactionTimeout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class ExecutionContextProcedureKernelTransaction implements KernelTransaction, TxStateHolder {
    private final KernelTransaction ktx;
    private final ExecutionContext ctx;
    private final long transactionSequenceNumberWhenCreated;

    public ExecutionContextProcedureKernelTransaction(KernelTransaction ktx, ExecutionContext ctx) {
        this.ktx = ktx;
        this.transactionSequenceNumberWhenCreated = ktx.getTransactionSequenceNumber();
        this.ctx = ctx;
    }

    public ExecutionContext executionContext() {
        return ctx;
    }

    @Override
    public long commit(KernelTransactionMonitor kernelTransactionMonitor) throws TransactionFailureException {
        throw new UnsupportedOperationException(
                "Committing ongoing transaction inside of a procedure or a function is unsupported.");
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException(
                "Rolling back ongoing transaction inside of a procedure or a function is unsupported.");
    }

    @Override
    public Read dataRead() {
        return ctx.dataRead();
    }

    @Override
    public Write dataWrite() throws InvalidTransactionTypeKernelException {
        throw failure("dataWrite");
    }

    @Override
    public TokenRead tokenRead() {
        return ctx.tokenRead();
    }

    @Override
    public TokenWrite tokenWrite() {
        throw failure("tokenWrite");
    }

    @Override
    public Token token() {
        throw failure("token");
    }

    @Override
    public SchemaRead schemaRead() {
        return ctx.schemaRead();
    }

    @Override
    public SchemaWrite schemaWrite() throws InvalidTransactionTypeKernelException {
        throw failure("schemaWrite");
    }

    @Override
    public Upgrade upgrade() {
        throw failure("upgrade");
    }

    @Override
    public Locks locks() {
        return ctx.locks();
    }

    @Override
    public CursorFactory cursors() {
        return ctx.cursors();
    }

    @Override
    public Procedures procedures() {
        return ctx.procedures();
    }

    @Override
    public ExecutionStatistics executionStatistics() {
        throw failure("executionStatistics");
    }

    @Override
    public long closeTransaction() throws TransactionFailureException {
        throw new UnsupportedOperationException(
                "Closing ongoing transaction inside of a procedure or a function is unsupported.");
    }

    @Override
    public void close() throws TransactionFailureException {
        throw new UnsupportedOperationException(
                "Closing ongoing transaction inside of a procedure or a function is unsupported.");
    }

    @Override
    public boolean isOpen() {
        return ktx.isOpen() && !ktx.isTerminated() && isOriginalTx();
    }

    @Override
    public boolean isClosing() {
        return ktx.isClosing() && isOriginalTx();
    }

    @Override
    public boolean isCommitting() {
        return ktx.isCommitting() && isOriginalTx();
    }

    @Override
    public boolean isRollingback() {
        return ktx.isRollingback() && isOriginalTx();
    }

    @Override
    public Optional<TerminationMark> getTerminationMark() {
        var mark = ktx.getTerminationMark();
        assertIsOriginalTx();
        return mark;
    }

    @Override
    public void releaseStorageEngineResources() {
        throw failure("releaseStorageEngineResources");
    }

    @Override
    public boolean isTerminated() {
        return ktx.isTerminated() && isOriginalTx();
    }

    @Override
    public void markForTermination(Status reason) {
        throw failure("markForTermination");
    }

    @Override
    public void setMetaData(Map<String, Object> data) {
        throw failure("setMetaData");
    }

    @Override
    public Map<String, Object> getMetaData() {
        throw failure("getMetaData");
    }

    @Override
    public void assertOpen() {
        ktx.assertOpen();
        assertIsOriginalTx();
    }

    @Override
    public void setStatusDetails(String details) {
        assertIsOriginalTx();
        ktx.setStatusDetails(details);
    }

    @Override
    public String statusDetails() {
        String details = ktx.statusDetails();
        assertIsOriginalTx();
        return details;
    }

    @Override
    public Statement acquireStatement() {
        throw failure("acquireStatement");
    }

    @Override
    public int aquireStatementCounter() {
        throw failure("aquireStatementCounter");
    }

    @Override
    public ResourceMonitor resourceMonitor() {
        return ctx;
    }

    @Override
    public IndexDescriptor indexUniqueCreate(IndexPrototype prototype) throws KernelException {
        throw failure("indexUniqueCreate");
    }

    @Override
    public SecurityContext securityContext() {
        return ctx.securityContext();
    }

    @Override
    public SecurityAuthorizationHandler securityAuthorizationHandler() {
        return ctx.securityAuthorizationHandler();
    }

    @Override
    public ClientConnectionInfo clientInfo() {
        throw failure("clientInfo");
    }

    @Override
    public AuthSubject subjectOrAnonymous() {
        throw failure("subjectOrAnonymous");
    }

    @Override
    public void bindToUserTransaction(InternalTransaction internalTransaction) {
        throw failure("bindToUserTransaction");
    }

    @Override
    public InternalTransaction internalTransaction() {
        return new ExecutionContextProcedureTransaction(this);
    }

    @Override
    public long startTime() {
        throw failure("startTime");
    }

    @Override
    public long startTimeNanos() {
        throw failure("startTimeNanos");
    }

    @Override
    public TransactionTimeout timeout() {
        throw failure("timeout");
    }

    @Override
    public Type transactionType() {
        throw failure("transactionType");
    }

    @Override
    public long getTransactionId() {
        var id = ktx.getTransactionId();
        assertIsOriginalTx();
        return id;
    }

    @Override
    public long getTransactionSequenceNumber() {
        var n = ktx.getTransactionSequenceNumber();
        assertIsOriginalTx();
        return n;
    }

    @Override
    public long getCommitTime() {
        throw failure("getCommitTime");
    }

    @Override
    public Revertable overrideWith(SecurityContext context) {
        throw failure("overrideWith");
    }

    @Override
    public ClockContext clocks() {
        throw failure("clocks");
    }

    @Override
    public NodeCursor ambientNodeCursor() {
        throw failure("ambientNodeCursor");
    }

    @Override
    public RelationshipScanCursor ambientRelationshipCursor() {
        throw failure("ambientRelationshipCursor");
    }

    @Override
    public PropertyCursor ambientPropertyCursor() {
        throw failure("ambientPropertyCursor");
    }

    @Override
    public boolean isSchemaTransaction() {
        return false;
    }

    @Override
    public CursorContext cursorContext() {
        return ctx.cursorContext();
    }

    @Override
    public ExecutionContext createExecutionContext() {
        throw failure("createExecutionContext");
    }

    @Override
    public MemoryTracker createExecutionContextMemoryTracker() {
        throw failure("createExecutionContextMemoryTracker");
    }

    @Override
    public QueryContext queryContext() {
        return ctx.queryContext();
    }

    @Override
    public StoreCursors storeCursors() {
        throw failure("storeCursors");
    }

    @Override
    public MemoryTracker memoryTracker() {
        return ctx.memoryTracker();
    }

    @Override
    public UUID getDatabaseId() {
        UUID uuid = ktx.getDatabaseId();
        assertIsOriginalTx();
        return uuid;
    }

    @Override
    public String getDatabaseName() {
        String dbName = ktx.getDatabaseName();
        assertIsOriginalTx();
        return dbName;
    }

    @Override
    public InnerTransactionHandler getInnerTransactionHandler() {
        throw failure("getInnerTransactionHandler");
    }

    // Since TX object is reused, let's check if this is still the same TX
    private boolean isOriginalTx() {
        return transactionSequenceNumberWhenCreated == ktx.getTransactionSequenceNumber();
    }

    private void assertIsOriginalTx() {
        if (!isOriginalTx()) {
            throw new IllegalStateException("Execution context used after transaction close");
        }
    }

    static UnsupportedOperationException failure(String op) {
        return new UnsupportedOperationException(
                "`transaction." + op
                        + "' is not supported in procedures when called from parallel runtime. Please retry using another runtime.");
    }

    @Override
    public TransactionState txState() {
        throw new UnsupportedOperationException("Accessing transaction state is not allowed during parallel execution");
    }

    @Override
    public boolean hasTxStateWithChanges() {
        return false;
    }
}
