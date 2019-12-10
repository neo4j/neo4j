/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.locking;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EventListener;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.LocksNotFrozenException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.query.statistic.StatisticProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.ValueMapper;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

@ImpermanentDbmsExtension
class QueryExecutionLocksIT
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void noLocksTakenForQueryWithoutAnyIndexesUsage() throws Exception
    {
        String query = "MATCH (n) return count(n)";
        List<LockOperationRecord> lockOperationRecords = traceQueryLocks( query );
        assertThat( lockOperationRecords ).as( "Observed list of lock operations is: " + lockOperationRecords ).isEmpty();
    }

    @Test
    void takeLabelLockForQueryWithIndexUsages() throws Exception
    {
        String labelName = "Human";
        Label human = Label.label( labelName );
        String propertyKey = "name";
        createIndex( human, propertyKey );

        try ( Transaction transaction = db.beginTx() )
        {
            Node node = transaction.createNode( human );
            node.setProperty( propertyKey, RandomStringUtils.randomAscii( 10 ) );
            transaction.commit();
        }

        String query = "MATCH (n:" + labelName + ") where n." + propertyKey + " = \"Fry\" RETURN n ";

        List<LockOperationRecord> lockOperationRecords = traceQueryLocks( query );
        assertThat( lockOperationRecords ).as( "Observed list of lock operations is: " + lockOperationRecords ).hasSize( 1 );

        LockOperationRecord operationRecord = lockOperationRecords.get( 0 );
        assertTrue( operationRecord.acquisition );
        assertFalse( operationRecord.exclusive );
        assertEquals( ResourceTypes.LABEL, operationRecord.resourceType );
    }

    @Test
    void reTakeLabelLockForQueryWithIndexUsagesWhenSchemaStateWasUpdatedDuringLockOperations() throws Exception
    {
        String labelName = "Robot";
        Label robot = Label.label( labelName );
        String propertyKey = "name";
        createIndex( robot, propertyKey );

        try ( Transaction transaction = db.beginTx() )
        {
            Node node = transaction.createNode( robot );
            node.setProperty( propertyKey, RandomStringUtils.randomAscii( 10 ) );
            transaction.commit();
        }

        String query = "MATCH (n:" + labelName + ") where n." + propertyKey + " = \"Bender\" RETURN n ";

        LockOperationListener lockOperationListener = new OnceSchemaFlushListener();
        List<LockOperationRecord> lockOperationRecords = traceQueryLocks( query, lockOperationListener );
        assertThat( lockOperationRecords ).as( "Observed list of lock operations is: " + lockOperationRecords ).hasSize( 3 );

        LockOperationRecord operationRecord = lockOperationRecords.get( 0 );
        assertTrue( operationRecord.acquisition );
        assertFalse( operationRecord.exclusive );
        assertEquals( ResourceTypes.LABEL, operationRecord.resourceType );

        LockOperationRecord operationRecord1 = lockOperationRecords.get( 1 );
        assertFalse( operationRecord1.acquisition );
        assertFalse( operationRecord1.exclusive );
        assertEquals( ResourceTypes.LABEL, operationRecord1.resourceType );

        LockOperationRecord operationRecord2 = lockOperationRecords.get( 2 );
        assertTrue( operationRecord2.acquisition );
        assertFalse( operationRecord2.exclusive );
        assertEquals( ResourceTypes.LABEL, operationRecord2.resourceType );
    }

    private void createIndex( Label label, String propertyKey )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.schema().indexFor( label ).on( propertyKey ).create();
            transaction.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }
    }

    private List<LockOperationRecord> traceQueryLocks( String query, LockOperationListener... listeners ) throws QueryExecutionKernelException
    {
        GraphDatabaseQueryService graph = db.getDependencyResolver().resolveDependency( GraphDatabaseQueryService.class );
        QueryExecutionEngine executionEngine = db.getDependencyResolver().resolveDependency( QueryExecutionEngine.class );
        try ( InternalTransaction tx = graph.beginTransaction( KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            TransactionalContextWrapper context = new TransactionalContextWrapper( createTransactionContext( graph, tx, query ), listeners );
            executionEngine.executeQuery( query, EMPTY_MAP, context, false );
            return new ArrayList<>( context.recordingLocks.getLockOperationRecords() );
        }
    }

    private static TransactionalContext createTransactionContext( GraphDatabaseQueryService graph, InternalTransaction tx, String query )
    {
        TransactionalContextFactory contextFactory = Neo4jTransactionalContextFactory.create( graph );
        return contextFactory.newContext( tx, query, EMPTY_MAP );
    }

    private static class TransactionalContextWrapper implements TransactionalContext
    {

        private final TransactionalContext delegate;
        private final List<LockOperationRecord> recordedLocks;
        private final LockOperationListener[] listeners;
        private RecordingLocks recordingLocks;

        private TransactionalContextWrapper( TransactionalContext delegate, LockOperationListener... listeners )
        {
            this( delegate, new ArrayList<>(), listeners );
        }

        private TransactionalContextWrapper( TransactionalContext delegate, List<LockOperationRecord> recordedLocks, LockOperationListener... listeners )
        {
            this.delegate = delegate;
            this.recordedLocks = recordedLocks;
            this.listeners = listeners;
        }

        @Override
        public ValueMapper<Object> valueMapper()
        {
            return delegate.valueMapper();
        }

        @Override
        public ExecutingQuery executingQuery()
        {
            return delegate.executingQuery();
        }

        @Override
        public DbmsOperations dbmsOperations()
        {
            return delegate.dbmsOperations();
        }

        @Override
        public KernelTransaction kernelTransaction()
        {
            if ( recordingLocks == null )
            {
                recordingLocks = new RecordingLocks( delegate.transaction(), asList( listeners ), recordedLocks );
            }
            return new DelegatingTransaction( delegate.kernelTransaction(), recordingLocks );
        }

        @Override
        public InternalTransaction transaction()
        {
            return delegate.transaction();
        }

        @Override
        public boolean isTopLevelTx()
        {
            return delegate.isTopLevelTx();
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public void rollback()
        {
            delegate.rollback();
        }

        @Override
        public void terminate()
        {
            delegate.terminate();
        }

        @Override
        public void commitAndRestartTx()
        {
            delegate.commitAndRestartTx();
        }

        @Override
        public TransactionalContext getOrBeginNewIfClosed()
        {
            if ( isOpen() )
            {
                return this;
            }
            else
            {
                return new TransactionalContextWrapper( delegate.getOrBeginNewIfClosed(), recordedLocks, listeners );
            }
        }

        @Override
        public boolean isOpen()
        {
            return delegate.isOpen();
        }

        @Override
        public GraphDatabaseQueryService graph()
        {
            return delegate.graph();
        }

        @Override
        public NamedDatabaseId databaseId()
        {
            return delegate.databaseId();
        }

        @Override
        public Statement statement()
        {
            return delegate.statement();
        }

        @Override
        public void check()
        {
            delegate.check();
        }

        @Override
        public SecurityContext securityContext()
        {
            return delegate.securityContext();
        }

        @Override
        public StatisticProvider kernelStatisticProvider()
        {
            return delegate.kernelStatisticProvider();
        }

        @Override
        public KernelTransaction.Revertable restrictCurrentTransaction( SecurityContext context )
        {
            return delegate.restrictCurrentTransaction( context );
        }

        @Override
        public ResourceTracker resourceTracker()
        {
            return delegate.resourceTracker();
        }
    }

    private static class RecordingLocks implements Locks
    {
        private final Locks delegate;
        private final List<LockOperationListener> listeners;
        private final List<LockOperationRecord> lockOperationRecords;
        private final InternalTransaction transaction;

        private RecordingLocks( InternalTransaction transaction, List<LockOperationListener> listeners, List<LockOperationRecord> lockOperationRecords )
        {
            this.listeners = listeners;
            this.lockOperationRecords = lockOperationRecords;
            this.transaction = transaction;
            this.delegate = transaction.kernelTransaction().locks();
        }

        List<LockOperationRecord> getLockOperationRecords()
        {
            return lockOperationRecords;
        }

        private void record( boolean exclusive, boolean acquisition, ResourceTypes type, long... ids )
        {
            if ( acquisition )
            {
                for ( LockOperationListener listener : listeners )
                {
                    listener.lockAcquired( transaction, exclusive, type, ids );
                }
            }
            lockOperationRecords.add( new LockOperationRecord( exclusive, acquisition, type, ids ) );
        }

        @Override
        public void acquireExclusiveNodeLock( long... ids )
        {
            record( true, true, ResourceTypes.NODE, ids );
            delegate.acquireExclusiveNodeLock( ids );
        }

        @Override
        public void acquireExclusiveRelationshipLock( long... ids )
        {
            record( true, true, ResourceTypes.RELATIONSHIP, ids );
            delegate.acquireExclusiveRelationshipLock( ids );
        }

        @Override
        public void acquireExclusiveLabelLock( long... ids )
        {
            record( true, true, ResourceTypes.LABEL, ids );
            delegate.acquireExclusiveLabelLock( ids );
        }

        @Override
        public void releaseExclusiveNodeLock( long... ids )
        {
            record( true, false, ResourceTypes.NODE, ids );
            delegate.releaseExclusiveNodeLock( ids );
        }

        @Override
        public void releaseExclusiveRelationshipLock( long... ids )
        {
            record( true, false, ResourceTypes.RELATIONSHIP, ids );
            delegate.releaseExclusiveRelationshipLock( ids );
        }

        @Override
        public void releaseExclusiveLabelLock( long... ids )
        {
            record( true, false, ResourceTypes.LABEL, ids );
            delegate.releaseExclusiveLabelLock( ids );
        }

        @Override
        public void acquireSharedNodeLock( long... ids )
        {
            record( false, true, ResourceTypes.NODE, ids );
            delegate.acquireSharedNodeLock( ids );
        }

        @Override
        public void acquireSharedRelationshipLock( long... ids )
        {
            record( false, true, ResourceTypes.RELATIONSHIP, ids );
            delegate.acquireSharedRelationshipLock( ids );
        }

        @Override
        public void acquireSharedLabelLock( long... ids )
        {
            record( false, true, ResourceTypes.LABEL, ids );
            delegate.acquireSharedLabelLock( ids );
        }

        @Override
        public void releaseSharedNodeLock( long... ids )
        {
            record( false, false, ResourceTypes.NODE, ids );
            delegate.releaseSharedNodeLock( ids );
        }

        @Override
        public void releaseSharedRelationshipLock( long... ids )
        {
            record( false, false, ResourceTypes.RELATIONSHIP, ids );
            delegate.releaseSharedRelationshipLock( ids );
        }

        @Override
        public void releaseSharedLabelLock( long... ids )
        {
            record( false, false, ResourceTypes.LABEL, ids );
            delegate.releaseSharedLabelLock( ids );
        }
    }

    private static class LockOperationListener implements EventListener
    {
        void lockAcquired( Transaction tx, boolean exclusive, ResourceType resourceType, long... ids )
        {
            // empty operation
        }
    }

    private static class LockOperationRecord
    {
        private final boolean exclusive;
        private final boolean acquisition;
        private final ResourceType resourceType;
        private final long[] ids;

        LockOperationRecord( boolean exclusive, boolean acquisition, ResourceType resourceType, long[] ids )
        {
            this.exclusive = exclusive;
            this.acquisition = acquisition;
            this.resourceType = resourceType;
            this.ids = ids;
        }

        @Override
        public String toString()
        {
            return "LockOperationRecord{" + "exclusive=" + exclusive + ", acquisition=" + acquisition +
                    ", resourceType=" + resourceType + ", ids=" + Arrays.toString( ids ) + '}';
        }
    }

    private static class OnceSchemaFlushListener extends LockOperationListener
    {
        private boolean executed;

        @Override
        void lockAcquired( Transaction tx, boolean exclusive, ResourceType resourceType, long... ids )
        {
            if ( !executed )
            {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
                ktx.schemaRead().schemaStateFlush();
            }
            executed = true;
        }
    }

    private static class DelegatingTransaction implements KernelTransaction
    {
        private final KernelTransaction internal;
        private final Locks locks;

        DelegatingTransaction( KernelTransaction internal, Locks locks )
        {
            this.internal = internal;
            this.locks = locks;
        }

        @Override
        public long commit() throws TransactionFailureException
        {
            return internal.commit();
        }

        @Override
        public void rollback() throws TransactionFailureException
        {
            internal.rollback();
        }

        @Override
        public Read dataRead()
        {
            return internal.dataRead();
        }

        @Override
        public Write dataWrite() throws InvalidTransactionTypeKernelException
        {
            return internal.dataWrite();
        }

        @Override
        public TokenRead tokenRead()
        {
            return internal.tokenRead();
        }

        @Override
        public TokenWrite tokenWrite()
        {
            return internal.tokenWrite();
        }

        @Override
        public org.neo4j.internal.kernel.api.Token token()
        {
            return internal.token();
        }

        @Override
        public SchemaRead schemaRead()
        {
            return internal.schemaRead();
        }

        @Override
        public SchemaWrite schemaWrite() throws InvalidTransactionTypeKernelException
        {
            return internal.schemaWrite();
        }

        @Override
        public Locks locks()
        {
            return locks;
        }

        @Override
        public void freezeLocks()
        {
            internal.freezeLocks();
        }

        @Override
        public void thawLocks() throws LocksNotFrozenException
        {
            internal.thawLocks();
        }

        @Override
        public CursorFactory cursors()
        {
            return internal.cursors();
        }

        @Override
        public Procedures procedures()
        {
            return internal.procedures();
        }

        @Override
        public ExecutionStatistics executionStatistics()
        {
            return internal.executionStatistics();
        }

        @Override
        public Statement acquireStatement()
        {
            return internal.acquireStatement();
        }

        @Override
        public IndexDescriptor indexUniqueCreate( IndexPrototype prototype ) throws KernelException
        {
            return internal.indexUniqueCreate( prototype );
        }

        @Override
        public long closeTransaction() throws TransactionFailureException
        {
            return internal.closeTransaction();
        }

        @Override
        public void close() throws TransactionFailureException
        {
            internal.close();
        }

        @Override
        public boolean isOpen()
        {
            return internal.isOpen();
        }

        @Override
        public boolean isClosing()
        {
            return internal.isClosing();
        }

        @Override
        public SecurityContext securityContext()
        {
            return internal.securityContext();
        }

        @Override
        public ClientConnectionInfo clientInfo()
        {
            return internal.clientInfo();
        }

        @Override
        public AuthSubject subjectOrAnonymous()
        {
            return internal.subjectOrAnonymous();
        }

        @Override
        public Optional<Status> getReasonIfTerminated()
        {
            return internal.getReasonIfTerminated();
        }

        @Override
        public boolean isTerminated()
        {
            return internal.isTerminated();
        }

        @Override
        public void markForTermination( Status reason )
        {
            internal.markForTermination( reason );
        }

        @Override
        public long lastTransactionTimestampWhenStarted()
        {
            return internal.lastTransactionTimestampWhenStarted();
        }

        @Override
        public long lastTransactionIdWhenStarted()
        {
            return internal.lastTransactionIdWhenStarted();
        }

        @Override
        public void bindToUserTransaction( InternalTransaction internalTransaction )
        {
            internal.bindToUserTransaction( internalTransaction );
        }

        @Override
        public InternalTransaction internalTransaction()
        {
            return internal.internalTransaction();
        }

        @Override
        public long startTime()
        {
            return internal.startTime();
        }

        @Override
        public long startTimeNanos()
        {
            return internal.startTimeNanos();
        }

        @Override
        public long timeout()
        {
            return internal.timeout();
        }

        @Override
        public Type transactionType()
        {
            return internal.transactionType();
        }

        @Override
        public long getTransactionId()
        {
            return internal.getTransactionId();
        }

        @Override
        public long getCommitTime()
        {
            return internal.getCommitTime();
        }

        @Override
        public Revertable overrideWith( SecurityContext context )
        {
            return internal.overrideWith( context );
        }

        @Override
        public ClockContext clocks()
        {
            return internal.clocks();
        }

        @Override
        public NodeCursor ambientNodeCursor()
        {
            return internal.ambientNodeCursor();
        }

        @Override
        public RelationshipScanCursor ambientRelationshipCursor()
        {
            return internal.ambientRelationshipCursor();
        }

        @Override
        public PropertyCursor ambientPropertyCursor()
        {
            return internal.ambientPropertyCursor();
        }

        @Override
        public void setMetaData( Map<String,Object> metaData )
        {
            internal.setMetaData( metaData );
        }

        @Override
        public Map<String,Object> getMetaData()
        {
            return internal.getMetaData();
        }

        @Override
        public void assertOpen()
        {
            internal.assertOpen();
        }

        @Override
        public boolean isSchemaTransaction()
        {
            return internal.isSchemaTransaction();
        }

        @Override
        public PageCursorTracer pageCursorTracer()
        {
            return null;
        }
    }
}
