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
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EventListener;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.internal.kernel.api.ExplicitIndexRead;
import org.neo4j.internal.kernel.api.ExplicitIndexWrite;
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
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.kernel.impl.query.statistic.StatisticProvider;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class QueryExecutionLocksIT
{

    @Rule
    public EmbeddedDatabaseRule databaseRule = new EmbeddedDatabaseRule();

    @Test
    public void noLocksTakenForQueryWithoutAnyIndexesUsage() throws Exception
    {
        String query = "MATCH (n) return count(n)";
        List<LockOperationRecord> lockOperationRecords = traceQueryLocks( query );
        assertThat( "Observed list of lock operations is: " + lockOperationRecords,
                lockOperationRecords, is( empty() ) );
    }

    @Test
    public void takeLabelLockForQueryWithIndexUsages() throws Exception
    {
        String labelName = "Human";
        Label human = Label.label( labelName );
        String propertyKey = "name";
        createIndex( human, propertyKey );

        try ( Transaction transaction = databaseRule.beginTx() )
        {
            Node node = databaseRule.createNode( human );
            node.setProperty( propertyKey, RandomStringUtils.randomAscii( 10 ) );
            transaction.success();
        }

        String query = "MATCH (n:" + labelName + ") where n." + propertyKey + " = \"Fry\" RETURN n ";

        List<LockOperationRecord> lockOperationRecords = traceQueryLocks( query );
        assertThat( "Observed list of lock operations is: " + lockOperationRecords,
                lockOperationRecords, hasSize( 1 ) );

        LockOperationRecord operationRecord = lockOperationRecords.get( 0 );
        assertTrue( operationRecord.acquisition );
        assertFalse( operationRecord.exclusive );
        assertEquals( ResourceTypes.LABEL, operationRecord.resourceType );
    }

    @Test
    public void reTakeLabelLockForQueryWithIndexUsagesWhenSchemaStateWasUpdatedDuringLockOperations() throws Exception
    {
        String labelName = "Robot";
        Label robot = Label.label( labelName );
        String propertyKey = "name";
        createIndex( robot, propertyKey );

        try ( Transaction transaction = databaseRule.beginTx() )
        {
            Node node = databaseRule.createNode( robot );
            node.setProperty( propertyKey, RandomStringUtils.randomAscii( 10 ) );
            transaction.success();
        }

        String query = "MATCH (n:" + labelName + ") where n." + propertyKey + " = \"Bender\" RETURN n ";

        LockOperationListener lockOperationListener = new OnceSchemaFlushListener();
        List<LockOperationRecord> lockOperationRecords = traceQueryLocks( query, lockOperationListener );
        assertThat( "Observed list of lock operations is: " + lockOperationRecords,
                lockOperationRecords, hasSize( 3 ) );

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
        try ( Transaction transaction = databaseRule.beginTx() )
        {
            databaseRule.schema().indexFor( label ).on( propertyKey ).create();
            transaction.success();
        }
        try ( Transaction ignored = databaseRule.beginTx() )
        {
            databaseRule.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }
    }

    private List<LockOperationRecord> traceQueryLocks( String query, LockOperationListener... listeners )
            throws QueryExecutionKernelException
    {
        GraphDatabaseQueryService graph = databaseRule.resolveDependency( GraphDatabaseQueryService.class );
        QueryExecutionEngine executionEngine = databaseRule.resolveDependency( QueryExecutionEngine.class );
        try ( InternalTransaction tx = graph
                .beginTransaction( KernelTransaction.Type.implicit, LoginContext.AUTH_DISABLED ) )
        {
            TransactionalContextWrapper context =
                    new TransactionalContextWrapper( createTransactionContext( graph, tx, query ), listeners );
            executionEngine.executeQuery( query, Collections.emptyMap(), context );
            return new ArrayList<>( context.recordingLocks.getLockOperationRecords() );
        }
    }

    private TransactionalContext createTransactionContext( GraphDatabaseQueryService graph, InternalTransaction tx,
            String query )
    {
        PropertyContainerLocker locker = new PropertyContainerLocker();
        TransactionalContextFactory contextFactory = Neo4jTransactionalContextFactory.create( graph, locker );
        return contextFactory.newContext( ClientConnectionInfo.EMBEDDED_CONNECTION, tx, query, EMPTY_MAP );
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
                recordingLocks =
                        new RecordingLocks( delegate.kernelTransaction().locks(), asList( listeners ), recordedLocks );
            }
            return new DelegatingTransaction( delegate.kernelTransaction(), recordingLocks );
        }

        @Override
        public boolean isTopLevelTx()
        {
            return delegate.isTopLevelTx();
        }

        @Override
        public void close( boolean success )
        {
            delegate.close( success );
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
        public void cleanForReuse()
        {
            delegate.cleanForReuse();
        }

        @Override
        public boolean twoLayerTransactionState()
        {
            return delegate.twoLayerTransactionState();
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
        public TxStateHolder stateView()
        {
            return delegate.stateView();
        }

        @Override
        public Lock acquireWriteLock( PropertyContainer p )
        {
            return delegate.acquireWriteLock( p );
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

        private RecordingLocks( Locks delegate,
                List<LockOperationListener> listeners,
                List<LockOperationRecord> lockOperationRecords )
        {
            this.delegate = delegate;
            this.listeners = listeners;
            this.lockOperationRecords = lockOperationRecords;
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
                    listener.lockAcquired( exclusive, type, ids );
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
        public void acquireExclusiveExplicitIndexLock( long... ids )
        {
            record( true, true, ResourceTypes.EXPLICIT_INDEX, ids );
            delegate.acquireExclusiveExplicitIndexLock( ids );
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
        public void releaseExclusiveExplicitIndexLock( long... ids )
        {
            record( true, false, ResourceTypes.EXPLICIT_INDEX, ids );
            delegate.releaseExclusiveExplicitIndexLock( ids );
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
        public void acquireSharedExplicitIndexLock( long... ids )
        {
            record( false, true, ResourceTypes.EXPLICIT_INDEX, ids );
            delegate.acquireSharedExplicitIndexLock( ids );
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
        public void releaseSharedExplicitIndexLock( long... ids )
        {
            record( false, false, ResourceTypes.EXPLICIT_INDEX, ids );
            delegate.releaseSharedExplicitIndexLock( ids );
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
        void lockAcquired( boolean exclusive, ResourceType resourceType, long... ids )
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

    private class OnceSchemaFlushListener extends LockOperationListener
    {
        private boolean executed;

        @Override
        void lockAcquired( boolean exclusive, ResourceType resourceType, long... ids )
        {
            if ( !executed )
            {
                ThreadToStatementContextBridge bridge =
                        databaseRule.resolveDependency( ThreadToStatementContextBridge.class );
                KernelTransaction ktx =
                        bridge.getKernelTransactionBoundToThisThread( true );
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
        public void success()
        {
            internal.success();
        }

        @Override
        public void failure()
        {
            internal.failure();
        }

        @Override
        public Read dataRead()
        {
            return internal.dataRead();
        }

        @Override
        public Read stableDataRead()
        {
            return internal.stableDataRead();
        }

        @Override
        public void markAsStable()
        {
            internal.markAsStable();
        }

        @Override
        public Write dataWrite() throws InvalidTransactionTypeKernelException
        {
            return internal.dataWrite();
        }

        @Override
        public ExplicitIndexRead indexRead()
        {
            return internal.indexRead();
        }

        @Override
        public ExplicitIndexWrite indexWrite() throws InvalidTransactionTypeKernelException
        {
            return internal.indexWrite();
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
        public SecurityContext securityContext()
        {
            return internal.securityContext();
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
        public long startTime()
        {
            return internal.startTime();
        }

        @Override
        public long timeout()
        {
            return internal.timeout();
        }

        @Override
        public void registerCloseListener( CloseListener listener )
        {
            internal.registerCloseListener( listener );
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
        public void assertOpen()
        {
            internal.assertOpen();
        }
    }
}
