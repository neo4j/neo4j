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
package org.neo4j.locking;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EventListener;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
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
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Upgrade;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.InnerTransactionHandler;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.TransactionTimeout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.query.statistic.StatisticProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineCostCharacteristics;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.ElementIdMapper;

@ImpermanentDbmsExtension
class QueryExecutionLocksIT {
    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private GraphDatabaseQueryService queryService;

    @Inject
    private QueryExecutionEngine executionEngine;

    @Test
    void noLocksTakenForQueryWithoutAnyIndexesUsage() throws Exception {
        String query = "MATCH (n) return count(n)";
        List<LockOperationRecord> lockOperationRecords = traceQueryLocks(query);
        assertThat(lockOperationRecords)
                .as("Observed list of lock operations is: " + lockOperationRecords)
                .isEmpty();
    }

    @Test
    void takeLabelLockForQueryWithIndexUsages() throws Exception {
        String labelName = "Human";
        Label human = Label.label(labelName);
        String propertyKey = "name";
        createIndex(human, propertyKey);

        try (Transaction transaction = db.beginTx()) {
            Node node = transaction.createNode(human);
            node.setProperty(propertyKey, RandomStringUtils.randomAscii(10));
            transaction.commit();
        }

        String query = "MATCH (n:" + labelName + ") where n." + propertyKey + " = \"Fry\" RETURN n ";

        List<LockOperationRecord> lockOperationRecords = traceQueryLocks(query);
        assertThat(lockOperationRecords)
                .as("Observed list of lock operations is: " + lockOperationRecords)
                .hasSize(1);

        LockOperationRecord operationRecord = lockOperationRecords.get(0);
        assertTrue(operationRecord.acquisition);
        assertFalse(operationRecord.exclusive);
        assertEquals(ResourceType.LABEL, operationRecord.resourceType);
    }

    @Test
    void takeRelationshipTypeLockForQueryWithIndexUsages() throws Exception {
        RelationshipType relType = RelationshipType.withName("REL");
        String propertyKey = "name";
        createRelationshipIndex(relType, propertyKey);

        try (Transaction transaction = db.beginTx()) {
            Node node1 = transaction.createNode();
            Node node2 = transaction.createNode();
            node1.createRelationshipTo(node2, relType).setProperty(propertyKey, "v");
            transaction.commit();
        }

        String query = "MATCH ()-[r:" + relType.name() + "]-() where r." + propertyKey + " = \"v\" RETURN r ";

        List<LockOperationRecord> lockOperationRecords = traceQueryLocks(query);
        assertThat(lockOperationRecords)
                .as("Observed list of lock operations is: " + lockOperationRecords)
                .hasSize(1);

        LockOperationRecord operationRecord = lockOperationRecords.get(0);
        assertTrue(operationRecord.acquisition);
        assertFalse(operationRecord.exclusive);
        assertEquals(ResourceType.RELATIONSHIP_TYPE, operationRecord.resourceType);
    }

    @Test
    void takeRelationshipTypeLockForQueryWithContainsScanIndexUsages() throws Exception {
        RelationshipType relType = RelationshipType.withName("REL");
        String propertyKey = "name";
        createRelationshipIndexWithType(relType, propertyKey, IndexType.TEXT);
        Relationship rel;
        try (Transaction transaction = db.beginTx()) {
            Node node1 = transaction.createNode();
            Node node2 = transaction.createNode();
            rel = node1.createRelationshipTo(node2, relType);
            rel.setProperty(propertyKey, "v");
            transaction.commit();
        }
        String query = "MATCH ()-[r:REL]->() WHERE r.name CONTAINS 'v' RETURN r.prop";

        List<LockOperationRecord> lockOperationRecords = traceQueryLocks(query);
        assertThat(lockOperationRecords)
                .as("Observed list of lock operations is: " + lockOperationRecords)
                .hasSize(1);

        LockOperationRecord operationRecord = lockOperationRecords.get(0);
        assertTrue(operationRecord.acquisition);
        assertFalse(operationRecord.exclusive);
        assertEquals(ResourceType.RELATIONSHIP_TYPE, operationRecord.resourceType);
    }

    @Test
    void takeRelationshipTypeLockForQueryWithEndsWithScanIndexUsages() throws Exception {
        RelationshipType relType = RelationshipType.withName("REL");
        String propertyKey = "name";
        createRelationshipIndexWithType(relType, propertyKey, IndexType.TEXT);
        Relationship rel;
        try (Transaction transaction = db.beginTx()) {
            Node node1 = transaction.createNode();
            Node node2 = transaction.createNode();
            rel = node1.createRelationshipTo(node2, relType);
            rel.setProperty(propertyKey, "v");
            transaction.commit();
        }
        String query = "MATCH ()-[r:REL]->() WHERE r.name ENDS WITH 'v' RETURN r.prop";

        List<LockOperationRecord> lockOperationRecords = traceQueryLocks(query);
        assertThat(lockOperationRecords)
                .as("Observed list of lock operations is: " + lockOperationRecords)
                .hasSize(1);

        LockOperationRecord operationRecord = lockOperationRecords.get(0);
        assertTrue(operationRecord.acquisition);
        assertFalse(operationRecord.exclusive);
        assertEquals(ResourceType.RELATIONSHIP_TYPE, operationRecord.resourceType);
    }

    @Test
    void takeRelationshipTypeLockForQueryWithSeekIndexUsages() throws Exception {
        RelationshipType relType = RelationshipType.withName("REL");
        String propertyKey = "name";
        createRelationshipIndex(relType, propertyKey);
        Relationship rel;
        try (Transaction transaction = db.beginTx()) {
            Node node1 = transaction.createNode();
            Node node2 = transaction.createNode();
            rel = node1.createRelationshipTo(node2, relType);
            rel.setProperty(propertyKey, "v");
            transaction.commit();
        }
        String query = "MATCH ()-[r:REL]->() WHERE r.name = 'v' RETURN r.prop";

        List<LockOperationRecord> lockOperationRecords = traceQueryLocks(query);
        assertThat(lockOperationRecords)
                .as("Observed list of lock operations is: " + lockOperationRecords)
                .hasSize(1);

        LockOperationRecord operationRecord = lockOperationRecords.get(0);
        assertTrue(operationRecord.acquisition);
        assertFalse(operationRecord.exclusive);
        assertEquals(ResourceType.RELATIONSHIP_TYPE, operationRecord.resourceType);
    }

    @Test
    void reTakeLabelLockForQueryWithIndexUsagesWhenSchemaStateWasUpdatedDuringLockOperations() throws Exception {
        String labelName = "Robot";
        Label robot = Label.label(labelName);
        String propertyKey = "name";
        createIndex(robot, propertyKey);

        try (Transaction transaction = db.beginTx()) {
            Node node = transaction.createNode(robot);
            node.setProperty(propertyKey, RandomStringUtils.randomAscii(10));
            transaction.commit();
        }

        String query = "MATCH (n:" + labelName + ") where n." + propertyKey + " = \"Bender\" RETURN n ";

        LockOperationListener lockOperationListener = new OnceSchemaFlushListener();
        List<LockOperationRecord> lockOperationRecords = traceQueryLocks(query, lockOperationListener);
        assertThat(lockOperationRecords)
                .as("Observed list of lock operations is: " + lockOperationRecords)
                .hasSize(3);

        LockOperationRecord operationRecord = lockOperationRecords.get(0);
        assertTrue(operationRecord.acquisition);
        assertFalse(operationRecord.exclusive);
        assertEquals(ResourceType.LABEL, operationRecord.resourceType);

        LockOperationRecord operationRecord1 = lockOperationRecords.get(1);
        assertFalse(operationRecord1.acquisition);
        assertFalse(operationRecord1.exclusive);
        assertEquals(ResourceType.LABEL, operationRecord1.resourceType);

        LockOperationRecord operationRecord2 = lockOperationRecords.get(2);
        assertTrue(operationRecord2.acquisition);
        assertFalse(operationRecord2.exclusive);
        assertEquals(ResourceType.LABEL, operationRecord2.resourceType);
    }

    @Test
    void labelScanWithLookupIndex() throws Exception {
        String labelName = "Robot";
        Label robot = Label.label(labelName);
        String propertyKey = "name";

        try (Transaction transaction = db.beginTx()) {
            Node node = transaction.createNode(robot);
            node.setProperty(propertyKey, RandomStringUtils.randomAscii(10));
            transaction.commit();
        }

        String query = "MATCH (n:" + labelName + ") RETURN n ";

        LockOperationListener lockOperationListener = new OnceSchemaFlushListener();
        List<LookupLockOperationRecord> lookupLockOperationRecords =
                traceLookupQueryLocks(query, lockOperationListener);
        assertThat(lookupLockOperationRecords)
                .as("Observed list of lookup lock operations is: " + lookupLockOperationRecords)
                .hasSize(3);

        LookupLockOperationRecord operationRecord = lookupLockOperationRecords.get(0);
        assertTrue(operationRecord.acquisition);
        assertFalse(operationRecord.exclusive);
        assertEquals(EntityType.NODE, operationRecord.entityType);

        LookupLockOperationRecord operationRecord1 = lookupLockOperationRecords.get(1);
        assertFalse(operationRecord1.acquisition);
        assertFalse(operationRecord1.exclusive);
        assertEquals(EntityType.NODE, operationRecord1.entityType);

        LookupLockOperationRecord operationRecord2 = lookupLockOperationRecords.get(2);
        assertTrue(operationRecord2.acquisition);
        assertFalse(operationRecord2.exclusive);
        assertEquals(EntityType.NODE, operationRecord2.entityType);
    }

    private void createIndex(Label label, String propertyKey) {
        try (Transaction transaction = db.beginTx()) {
            transaction.schema().indexFor(label).on(propertyKey).create();
            transaction.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
        }
    }

    private void createRelationshipIndex(RelationshipType relType, String propertyKey) {
        createRelationshipIndexWithType(relType, propertyKey, IndexType.RANGE);
    }

    private void createRelationshipIndexWithType(RelationshipType relType, String propertyKey, IndexType indexType) {
        try (Transaction transaction = db.beginTx()) {
            transaction
                    .schema()
                    .indexFor(relType)
                    .on(propertyKey)
                    .withIndexType(indexType)
                    .create();
            transaction.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
        }
    }

    private List<LockOperationRecord> traceQueryLocks(String query, LockOperationListener... listeners)
            throws QueryExecutionKernelException {
        try (InternalTransaction tx =
                queryService.beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED)) {
            TransactionalContextWrapper context =
                    new TransactionalContextWrapper(createTransactionContext(queryService, tx, query), listeners);
            executionEngine.executeQuery(query, EMPTY_MAP, context, false);
            return new ArrayList<>(context.recordingLocks.getLockOperationRecords());
        }
    }

    private List<LookupLockOperationRecord> traceLookupQueryLocks(String query, LockOperationListener... listeners)
            throws QueryExecutionKernelException {
        try (InternalTransaction tx =
                queryService.beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED)) {
            TransactionalContextWrapper context =
                    new TransactionalContextWrapper(createTransactionContext(queryService, tx, query), listeners);
            executionEngine.executeQuery(query, EMPTY_MAP, context, false);
            return new ArrayList<>(context.recordingLocks.getLookupLockOperationRecords());
        }
    }

    private static TransactionalContext createTransactionContext(
            GraphDatabaseQueryService graph, InternalTransaction tx, String query) {
        TransactionalContextFactory contextFactory = Neo4jTransactionalContextFactory.create(graph);
        return contextFactory.newContext(tx, query, EMPTY_MAP, QueryExecutionConfiguration.DEFAULT_CONFIG);
    }

    private static class TransactionalContextWrapper implements TransactionalContext {

        private final TransactionalContext delegate;
        private final List<LockOperationRecord> recordedLocks;
        private final List<LookupLockOperationRecord> recordedLookupLocks;
        private final LockOperationListener[] listeners;
        private RecordingLocks recordingLocks;

        private TransactionalContextWrapper(TransactionalContext delegate, LockOperationListener... listeners) {
            this(delegate, new ArrayList<>(), new ArrayList<>(), listeners);
        }

        private TransactionalContextWrapper(
                TransactionalContext delegate,
                List<LockOperationRecord> recordedLocks,
                List<LookupLockOperationRecord> recordedLookupLocks,
                LockOperationListener... listeners) {
            this.delegate = delegate;
            this.recordedLocks = recordedLocks;
            this.listeners = listeners;
            this.recordedLookupLocks = recordedLookupLocks;
        }

        @Override
        public ExecutingQuery executingQuery() {
            return delegate.executingQuery();
        }

        @Override
        public KernelTransaction kernelTransaction() {
            if (recordingLocks == null) {
                recordingLocks = new RecordingLocks(
                        delegate.transaction(), asList(listeners), recordedLocks, recordedLookupLocks);
            }
            return new DelegatingTransaction(delegate.kernelTransaction(), recordingLocks);
        }

        @Override
        public InternalTransaction transaction() {
            return delegate.transaction();
        }

        @Override
        public boolean isTopLevelTx() {
            return delegate.isTopLevelTx();
        }

        @Override
        public ConstituentTransactionFactory constituentTransactionFactory() {
            return delegate.constituentTransactionFactory();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public void commit() {
            delegate.commit();
        }

        @Override
        public void rollback() {
            delegate.rollback();
        }

        @Override
        public void terminate() {
            delegate.terminate();
        }

        @Override
        public long commitAndRestartTx() {
            return delegate.commitAndRestartTx();
        }

        @Override
        public TransactionalContext getOrBeginNewIfClosed() {
            if (isOpen()) {
                return this;
            } else {
                return new TransactionalContextWrapper(
                        delegate.getOrBeginNewIfClosed(), recordedLocks, recordedLookupLocks, listeners);
            }
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public GraphDatabaseQueryService graph() {
            return delegate.graph();
        }

        @Override
        public NamedDatabaseId databaseId() {
            return delegate.databaseId();
        }

        @Override
        public Statement statement() {
            return delegate.statement();
        }

        @Override
        public SecurityContext securityContext() {
            return delegate.securityContext();
        }

        @Override
        public StatisticProvider kernelStatisticProvider() {
            return delegate.kernelStatisticProvider();
        }

        @Override
        public KernelTransaction.Revertable restrictCurrentTransaction(SecurityContext context) {
            return delegate.restrictCurrentTransaction(context);
        }

        @Override
        public ResourceTracker resourceTracker() {
            return delegate.resourceTracker();
        }

        @Override
        public TransactionalContext contextWithNewTransaction() {
            return new TransactionalContextWrapper(
                    delegate.contextWithNewTransaction(), recordedLocks, recordedLookupLocks, listeners);
        }

        @Override
        public ElementIdMapper elementIdMapper() {
            return delegate.elementIdMapper();
        }

        @Override
        public QueryExecutionConfiguration queryExecutingConfiguration() {
            return QueryExecutionConfiguration.DEFAULT_CONFIG;
        }
    }

    private static class RecordingLocks implements Locks {
        private final Locks delegate;
        private final List<LockOperationListener> listeners;
        private final List<LockOperationRecord> lockOperationRecords;
        private final List<LookupLockOperationRecord> lookupLockOperationRecords;
        private final InternalTransaction transaction;

        private RecordingLocks(
                InternalTransaction transaction,
                List<LockOperationListener> listeners,
                List<LockOperationRecord> lockOperationRecords,
                List<LookupLockOperationRecord> lookupLockOperationRecords) {
            this.listeners = listeners;
            this.lockOperationRecords = lockOperationRecords;
            this.lookupLockOperationRecords = lookupLockOperationRecords;
            this.transaction = transaction;
            this.delegate = transaction.kernelTransaction().locks();
        }

        List<LockOperationRecord> getLockOperationRecords() {
            return lockOperationRecords;
        }

        List<LookupLockOperationRecord> getLookupLockOperationRecords() {
            return lookupLockOperationRecords;
        }

        private void record(boolean exclusive, boolean acquisition, ResourceType type, long... ids) {
            if (acquisition) {
                for (LockOperationListener listener : listeners) {
                    listener.lockAcquired(transaction, exclusive, type, ids);
                }
            }
            lockOperationRecords.add(new LockOperationRecord(exclusive, acquisition, type, ids));
        }

        private void recordLookupIndex(boolean exclusive, boolean acquisition, EntityType type) {
            if (acquisition) {
                for (LockOperationListener listener : listeners) {
                    listener.lockAcquired(transaction, exclusive, type);
                }
            }
            lookupLockOperationRecords.add(new LookupLockOperationRecord(exclusive, acquisition, type));
        }

        @Override
        public void acquireExclusiveNodeLock(long... ids) {
            record(true, true, ResourceType.NODE, ids);
            delegate.acquireExclusiveNodeLock(ids);
        }

        @Override
        public void acquireExclusiveRelationshipLock(long... ids) {
            record(true, true, ResourceType.RELATIONSHIP, ids);
            delegate.acquireExclusiveRelationshipLock(ids);
        }

        @Override
        public void releaseExclusiveNodeLock(long... ids) {
            record(true, false, ResourceType.NODE, ids);
            delegate.releaseExclusiveNodeLock(ids);
        }

        @Override
        public void releaseExclusiveRelationshipLock(long... ids) {
            record(true, false, ResourceType.RELATIONSHIP, ids);
            delegate.releaseExclusiveRelationshipLock(ids);
        }

        @Override
        public void acquireSharedNodeLock(long... ids) {
            record(false, true, ResourceType.NODE, ids);
            delegate.acquireSharedNodeLock(ids);
        }

        @Override
        public void acquireSharedRelationshipLock(long... ids) {
            record(false, true, ResourceType.RELATIONSHIP, ids);
            delegate.acquireSharedRelationshipLock(ids);
        }

        @Override
        public void acquireSharedLabelLock(long... ids) {
            record(false, true, ResourceType.LABEL, ids);
            delegate.acquireSharedLabelLock(ids);
        }

        @Override
        public void acquireSharedRelationshipTypeLock(long... ids) {
            record(false, true, ResourceType.RELATIONSHIP_TYPE, ids);
            delegate.acquireSharedRelationshipTypeLock(ids);
        }

        @Override
        public void releaseSharedNodeLock(long... ids) {
            record(false, false, ResourceType.NODE, ids);
            delegate.releaseSharedNodeLock(ids);
        }

        @Override
        public void releaseSharedRelationshipLock(long... ids) {
            record(false, false, ResourceType.RELATIONSHIP, ids);
            delegate.releaseSharedRelationshipLock(ids);
        }

        @Override
        public void releaseSharedLabelLock(long... ids) {
            record(false, false, ResourceType.LABEL, ids);
            delegate.releaseSharedLabelLock(ids);
        }

        @Override
        public void releaseSharedRelationshipTypeLock(long... ids) {
            record(false, false, ResourceType.RELATIONSHIP_TYPE, ids);
            delegate.releaseSharedRelationshipTypeLock(ids);
        }

        @Override
        public void acquireSharedLookupLock(EntityType entityType) {
            recordLookupIndex(false, true, entityType);
            delegate.acquireSharedLookupLock(entityType);
        }

        @Override
        public void releaseSharedLookupLock(EntityType entityType) {
            recordLookupIndex(false, false, entityType);
            delegate.releaseSharedLookupLock(entityType);
        }

        @Override
        public void releaseExclusiveIndexEntryLock(long... indexEntries) {
            delegate.releaseExclusiveIndexEntryLock(indexEntries);
        }

        @Override
        public void acquireExclusiveIndexEntryLock(long... indexEntries) {
            delegate.acquireExclusiveIndexEntryLock(indexEntries);
        }

        @Override
        public void releaseSharedIndexEntryLock(long... indexEntries) {
            delegate.releaseSharedIndexEntryLock(indexEntries);
        }

        @Override
        public void acquireSharedIndexEntryLock(long... indexEntries) {
            delegate.acquireSharedIndexEntryLock(indexEntries);
        }

        @Override
        public void acquireSharedSchemaLock(SchemaDescriptorSupplier schemaLike) {
            delegate.acquireSharedSchemaLock(schemaLike);
        }

        @Override
        public void releaseSharedSchemaLock(SchemaDescriptorSupplier schemaLike) {
            delegate.releaseSharedSchemaLock(schemaLike);
        }
    }

    private static class LockOperationListener implements EventListener {
        void lockAcquired(Transaction tx, boolean exclusive, ResourceType resourceType, long... ids) {
            // empty operation
        }

        void lockAcquired(Transaction tx, boolean exclusive, EntityType resourceType) {
            // empty operation
        }
    }

    private static class LockOperationRecord {
        private final boolean exclusive;
        private final boolean acquisition;
        private final ResourceType resourceType;
        private final long[] ids;

        LockOperationRecord(boolean exclusive, boolean acquisition, ResourceType resourceType, long[] ids) {
            this.exclusive = exclusive;
            this.acquisition = acquisition;
            this.resourceType = resourceType;
            this.ids = ids;
        }

        @Override
        public String toString() {
            return "LockOperationRecord{" + "exclusive=" + exclusive + ", acquisition=" + acquisition
                    + ", resourceType=" + resourceType + ", ids=" + Arrays.toString(ids) + '}';
        }
    }

    private static class LookupLockOperationRecord {
        private final boolean exclusive;
        private final boolean acquisition;
        private final EntityType entityType;

        LookupLockOperationRecord(boolean exclusive, boolean acquisition, EntityType entityType) {
            this.exclusive = exclusive;
            this.acquisition = acquisition;
            this.entityType = entityType;
        }

        @Override
        public String toString() {
            return "LookupLockOperationRecord{exclusive=" + exclusive + ", acquisition=" + acquisition + ", entityType="
                    + entityType + "}";
        }
    }

    private static class OnceSchemaFlushListener extends LockOperationListener {
        private boolean executed;

        @Override
        void lockAcquired(Transaction tx, boolean exclusive, ResourceType resourceType, long... ids) {
            if (!executed) {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
                ktx.schemaRead().schemaStateFlush();
            }
            executed = true;
        }

        @Override
        void lockAcquired(Transaction tx, boolean exclusive, EntityType type) {
            if (!executed) {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
                ktx.schemaRead().schemaStateFlush();
            }
            executed = true;
        }
    }

    private static class DelegatingTransaction implements KernelTransaction {
        private final KernelTransaction internal;
        private final Locks locks;

        DelegatingTransaction(KernelTransaction internal, Locks locks) {
            this.internal = internal;
            this.locks = locks;
        }

        @Override
        public long commit(KernelTransactionMonitor kernelTransactionMonitor) throws TransactionFailureException {
            return internal.commit(kernelTransactionMonitor);
        }

        @Override
        public void rollback() throws TransactionFailureException {
            internal.rollback();
        }

        @Override
        public Read dataRead() {
            return internal.dataRead();
        }

        @Override
        public Write dataWrite() throws InvalidTransactionTypeKernelException {
            return internal.dataWrite();
        }

        @Override
        public TokenRead tokenRead() {
            return internal.tokenRead();
        }

        @Override
        public TokenWrite tokenWrite() {
            return internal.tokenWrite();
        }

        @Override
        public org.neo4j.internal.kernel.api.Token token() {
            return internal.token();
        }

        @Override
        public SchemaRead schemaRead() {
            return internal.schemaRead();
        }

        @Override
        public SchemaWrite schemaWrite() throws InvalidTransactionTypeKernelException {
            return internal.schemaWrite();
        }

        @Override
        public Upgrade upgrade() {
            return internal.upgrade();
        }

        @Override
        public Locks locks() {
            return locks;
        }

        @Override
        public CursorFactory cursors() {
            return internal.cursors();
        }

        @Override
        public Procedures procedures() {
            return internal.procedures();
        }

        @Override
        public ExecutionStatistics executionStatistics() {
            return internal.executionStatistics();
        }

        @Override
        public StorageEngineCostCharacteristics storageEngineCostCharacteristics() {
            return internal.storageEngineCostCharacteristics();
        }

        @Override
        public Statement acquireStatement() {
            return internal.acquireStatement();
        }

        @Override
        public int aquireStatementCounter() {
            return internal.aquireStatementCounter();
        }

        @Override
        public ResourceMonitor resourceMonitor() {
            return internal.resourceMonitor();
        }

        @Override
        public IndexDescriptor indexUniqueCreate(IndexPrototype prototype) throws KernelException {
            return internal.indexUniqueCreate(prototype);
        }

        @Override
        public long closeTransaction() throws TransactionFailureException {
            return internal.closeTransaction();
        }

        @Override
        public void close() throws TransactionFailureException {
            internal.close();
        }

        @Override
        public boolean isOpen() {
            return internal.isOpen();
        }

        @Override
        public boolean isCommitting() {
            return internal.isCommitting();
        }

        @Override
        public boolean isRollingback() {
            return internal.isRollingback();
        }

        @Override
        public boolean isClosing() {
            return internal.isClosing();
        }

        @Override
        public SecurityContext securityContext() {
            return internal.securityContext();
        }

        @Override
        public SecurityAuthorizationHandler securityAuthorizationHandler() {
            return internal.securityAuthorizationHandler();
        }

        @Override
        public ClientConnectionInfo clientInfo() {
            return internal.clientInfo();
        }

        @Override
        public AuthSubject subjectOrAnonymous() {
            return internal.subjectOrAnonymous();
        }

        @Override
        public Optional<TerminationMark> getTerminationMark() {
            return internal.getTerminationMark();
        }

        @Override
        public void releaseStorageEngineResources() {
            internal.releaseStorageEngineResources();
        }

        @Override
        public boolean isTerminated() {
            return internal.isTerminated();
        }

        @Override
        public void markForTermination(Status reason) {
            internal.markForTermination(reason);
        }

        @Override
        public void bindToUserTransaction(InternalTransaction internalTransaction) {
            internal.bindToUserTransaction(internalTransaction);
        }

        @Override
        public InternalTransaction internalTransaction() {
            return internal.internalTransaction();
        }

        @Override
        public long startTime() {
            return internal.startTime();
        }

        @Override
        public long startTimeNanos() {
            return internal.startTimeNanos();
        }

        @Override
        public TransactionTimeout timeout() {
            return internal.timeout();
        }

        @Override
        public Type transactionType() {
            return internal.transactionType();
        }

        @Override
        public long getTransactionId() {
            return internal.getTransactionId();
        }

        @Override
        public long getTransactionSequenceNumber() {
            return internal.getTransactionSequenceNumber();
        }

        @Override
        public long getCommitTime() {
            return internal.getCommitTime();
        }

        @Override
        public Revertable overrideWith(SecurityContext context) {
            return internal.overrideWith(context);
        }

        @Override
        public ClockContext clocks() {
            return internal.clocks();
        }

        @Override
        public NodeCursor ambientNodeCursor() {
            return internal.ambientNodeCursor();
        }

        @Override
        public RelationshipScanCursor ambientRelationshipCursor() {
            return internal.ambientRelationshipCursor();
        }

        @Override
        public PropertyCursor ambientPropertyCursor() {
            return internal.ambientPropertyCursor();
        }

        @Override
        public void setMetaData(Map<String, Object> metaData) {
            internal.setMetaData(metaData);
        }

        @Override
        public Map<String, Object> getMetaData() {
            return internal.getMetaData();
        }

        @Override
        public void setStatusDetails(String statusDetails) {
            internal.setStatusDetails(statusDetails);
        }

        @Override
        public String statusDetails() {
            return internal.statusDetails();
        }

        @Override
        public void assertOpen() {
            internal.assertOpen();
        }

        @Override
        public boolean isSchemaTransaction() {
            return internal.isSchemaTransaction();
        }

        @Override
        public CursorContext cursorContext() {
            return null;
        }

        @Override
        public ExecutionContext createExecutionContext() {
            return internal.createExecutionContext();
        }

        @Override
        public MemoryTracker createExecutionContextMemoryTracker() {
            return internal.createExecutionContextMemoryTracker();
        }

        @Override
        public QueryContext queryContext() {
            return internal.queryContext();
        }

        @Override
        public StoreCursors storeCursors() {
            return null;
        }

        @Override
        public MemoryTracker memoryTracker() {
            return EmptyMemoryTracker.INSTANCE;
        }

        @Override
        public UUID getDatabaseId() {
            return null;
        }

        @Override
        public String getDatabaseName() {
            return null;
        }

        @Override
        public InnerTransactionHandler getInnerTransactionHandler() {
            return internal.getInnerTransactionHandler();
        }
    }
}
