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
package org.neo4j.procedure.builtin;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class StubKernelTransaction implements KernelTransaction {

    StubKernelTransaction() {}

    @Override
    public Statement acquireStatement() {
        return new StubStatement();
    }

    @Override
    public int aquireStatementCounter() {
        return 0;
    }

    @Override
    public ResourceMonitor resourceMonitor() {
        return acquireStatement();
    }

    @Override
    public IndexDescriptor indexUniqueCreate(IndexPrototype prototype) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public long commit(KernelTransactionMonitor kernelTransactionMonitor) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Read dataRead() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Write dataWrite() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public TokenRead tokenRead() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public TokenWrite tokenWrite() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Token token() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public SchemaRead schemaRead() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public SchemaWrite schemaWrite() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Upgrade upgrade() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Locks locks() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public CursorFactory cursors() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Procedures procedures() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public ExecutionStatistics executionStatistics() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public long closeTransaction() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void close() throws TransactionFailureException {}

    @Override
    public boolean isOpen() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean isClosing() {
        return false;
    }

    @Override
    public boolean isCommitting() {
        return false;
    }

    @Override
    public boolean isRollingback() {
        return false;
    }

    @Override
    public SecurityContext securityContext() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public SecurityAuthorizationHandler securityAuthorizationHandler() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public ClientConnectionInfo clientInfo() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public AuthSubject subjectOrAnonymous() {
        AuthSubject subject = mock(AuthSubject.class);
        when(subject.executingUser()).thenReturn("testUser");
        return subject;
    }

    @Override
    public Optional<TerminationMark> getTerminationMark() {
        return Optional.empty();
    }

    @Override
    public void releaseStorageEngineResources() {}

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public void markForTermination(Status reason) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void bindToUserTransaction(InternalTransaction internalTransaction) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public InternalTransaction internalTransaction() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public long startTime() {
        return 1984;
    }

    @Override
    public long startTimeNanos() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public TransactionTimeout timeout() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Type transactionType() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public long getTransactionId() {
        return 8;
    }

    @Override
    public long getTransactionSequenceNumber() {
        return 8;
    }

    @Override
    public long getCommitTime() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Revertable overrideWith(SecurityContext context) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public ClockContext clocks() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public NodeCursor ambientNodeCursor() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public RelationshipScanCursor ambientRelationshipCursor() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public PropertyCursor ambientPropertyCursor() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void setMetaData(Map<String, Object> metaData) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Map<String, Object> getMetaData() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void setStatusDetails(String statusDetails) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public String statusDetails() {
        return null;
    }

    @Override
    public void assertOpen() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean isSchemaTransaction() {
        return false;
    }

    @Override
    public CursorContext cursorContext() {
        return null;
    }

    @Override
    public ExecutionContext createExecutionContext() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public MemoryTracker createExecutionContextMemoryTracker() {
        return null;
    }

    @Override
    public QueryContext queryContext() {
        return null;
    }

    @Override
    public StoreCursors storeCursors() {
        return null;
    }

    @Override
    public MemoryTracker memoryTracker() {
        return null;
    }

    @Override
    public UUID getDatabaseId() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public String getDatabaseName() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public InnerTransactionHandler getInnerTransactionHandler() {
        throw new UnsupportedOperationException("not implemented");
    }
}
