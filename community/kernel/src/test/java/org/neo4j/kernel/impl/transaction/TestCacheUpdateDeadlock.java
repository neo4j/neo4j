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
package org.neo4j.kernel.impl.transaction;

import org.junit.Ignore;


/**
Found one Java-level deadlock:
=============================
"pool-1-thread-16":
  waiting to lock monitor 0x00007fdc000cdc08 (object 0x00000000d9e45558, a org.neo4j.kernel.impl.core.NodeImpl),
  which is held by "pool-1-thread-12"
"pool-1-thread-12":
  waiting to lock monitor 0x00007fdbf80078e8 (object 0x000000008c41bc78, a org.neo4j.kernel.impl.transaction.xaframework.XaRes
ourceManager),
  which is held by "pool-1-thread-16"

Java stack information for the threads listed above:
===================================================
"pool-1-thread-16":
        at org.neo4j.kernel.impl.core.ArrayBasedPrimitive.commitPropertyMaps(ArrayBasedPrimitive.java:206)
        - waiting to lock <0x00000000d9e45558> (a org.neo4j.kernel.impl.core.NodeImpl)
        at org.neo4j.kernel.impl.core.WritableTransactionState.releaseCows(WritableTransactionState.java:443)
        at org.neo4j.kernel.impl.core.WritableTransactionState.commitCows(WritableTransactionState.java:378)
        at org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransaction.applyCommit(NeoStoreTransaction.java:857)
        at org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransaction.doCommit(NeoStoreTransaction.java:752)
        at org.neo4j.kernel.impl.transaction.xaframework.XaTransaction.commit(XaTransaction.java:327)
        at org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager.commitWriteTx(XaResourceManager.java:579)
        at org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager.commit(XaResourceManager.java:490)
        - locked <0x000000008c41bc78> (a org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager)
        at org.neo4j.kernel.impl.transaction.xaframework.XaResourceHelpImpl.commit(XaResourceHelpImpl.java:64)
        at org.neo4j.kernel.impl.transaction.TransactionImpl.doCommit(TransactionImpl.java:544)
        at org.neo4j.kernel.impl.transaction.TxManager.commit(TxManager.java:464)
        at org.neo4j.kernel.impl.transaction.TxManager.commit(TxManager.java:403)
        at org.neo4j.kernel.impl.transaction.TransactionImpl.commit(TransactionImpl.java:123)
        - locked <0x00000000db2f8030> (a org.neo4j.kernel.impl.transaction.TransactionImpl)
        at org.neo4j.kernel.TopLevelTransaction.close(TopLevelTransaction.java:124)
        at org.neo4j.kernel.TopLevelTransaction.finish(TopLevelTransaction.java:111)
        at org.neo4j.bench.BasicConcurrencyBenchmark$Worker.run(BasicConcurrencyBenchmark.java:399)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:471)
        at java.util.concurrent.FutureTask.run(FutureTask.java:262)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
        at java.lang.Thread.run(Thread.java:744)
"pool-1-thread-12":
        at org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager.createTransaction(XaResourceManager.java:108)
        - waiting to lock <0x000000008c41bc78> (a org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager)
        at org.neo4j.kernel.impl.transaction.xaframework.XaConnectionHelpImpl.createTransaction(XaConnectionHelpImpl.java:139)
        at org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaConnection.createTransaction(NeoStoreXaConnection.java:89)
        at org.neo4j.kernel.impl.nioneo.xa.NioNeoDbPersistenceSource.createTransaction(NioNeoDbPersistenceSource.java:48)
        at org.neo4j.kernel.impl.persistence.PersistenceManager.createResource(PersistenceManager.java:259)
        at org.neo4j.kernel.impl.persistence.PersistenceManager.getResource(PersistenceManager.java:249)
        at org.neo4j.kernel.impl.persistence.PersistenceManager.getRelationshipChainPosition(PersistenceManager.java:88)
        at org.neo4j.kernel.impl.core.NodeManager.getRelationshipChainPosition(NodeManager.java:580)
        at org.neo4j.kernel.impl.core.NodeImpl.loadInitialRelationships(NodeImpl.java:385)
        - locked <0x00000000d9e45558> (a org.neo4j.kernel.impl.core.NodeImpl)
        at org.neo4j.kernel.impl.core.NodeImpl.ensureRelationshipMapNotNull(NodeImpl.java:372)
        at org.neo4j.kernel.impl.core.NodeImpl.getAllRelationships(NodeImpl.java:150)
        at org.neo4j.kernel.impl.core.NodeImpl.getRelationships(NodeImpl.java:303)
        at org.neo4j.kernel.impl.core.NodeProxy.getRelationships(NodeProxy.java:119)
        at org.neo4j.bench.BasicConcurrencyBenchmark$3.processRandomNode(BasicConcurrencyBenchmark.java:252)
        at org.neo4j.bench.BasicConcurrencyBenchmark$Worker.run(BasicConcurrencyBenchmark.java:387)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:471)
        at java.util.concurrent.FutureTask.run(FutureTask.java:262)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
        at java.lang.Thread.run(Thread.java:744)

Found 1 deadlock. */

// TODO 2.2-future figure out if this is necessary and rewrite it
@Ignore("Fix this for 2.2")
public class TestCacheUpdateDeadlock
{
    /*
    @Test
    public void shouldNotHaveLoaderDeadlockWithCommitter() throws Exception
    {
        // GIVEN a thread that is committing
        GraphDatabaseAPI db = newDeadlockProneDb();
        cleanup.add( closeable( db ) );
        Node node = createCleanNode( db );
        DoubleLatch deadlockLatch = testStateFactory.installDeadlockLatch();
        OtherThreadExecutor<Void> committer = cleanup.add( new OtherThreadExecutor<Void>( "Committer",
                5, SECONDS, null ) );
        Future<Object> commitFuture = committer.executeDontWait( setProperty( db, node ) );
        deadlockLatch.awaitStart();

        // -- and another one starting to load from store (creating the transaction)
        OtherThreadExecutor<Void> reader = cleanup.add( new OtherThreadExecutor<Void>( "Reader",
                5, SECONDS, null ) );
        Future<Object> readerFuture = reader.executeDontWait( readRelationships( db, node ) );
        reader.waitUntil( reader.orExecutionCompleted( anyThreadState( BLOCKED ) ) );

        // WHEN the committing thread is updating the cache
        deadlockLatch.finish();

        // THEN it should not deadlock with the loader
        tryAwait( readerFuture, 5, SECONDS );
        tryAwait( commitFuture, 5, SECONDS );
        assertFalse( "Should not have dead locked the JVM", jvmLevelDeadlockFound() );
    }

    private void tryAwait( Future<?> future, int timeout, TimeUnit unit )
    {
        try
        {
            future.get( timeout, unit );
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new RuntimeException( e );
        }
        catch ( TimeoutException e )
        {   // OK
        }
    }

    private boolean jvmLevelDeadlockFound()
    {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long[] threadIds = bean.findDeadlockedThreads();
        return threadIds != null;
    }

    private Closeable closeable( final GraphDatabaseAPI db )
    {
        return new Closeable()
        {
            @Override
            public void close() throws IOException
            {
                db.shutdown();
            }
        };
    }

    private Node createCleanNode( GraphDatabaseAPI db )
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( "name", "not a value" );
            tx.success();
        }
        // TODO 2.2-future Clear cache
        try ( Transaction tx = db.beginTx() )
        {   // transaction that will load in the node, although empty
            db.getNodeById( node.getId() );
            tx.success();
        }
        return node;
    }

    private WorkerCommand<Void, Object> readRelationships( final GraphDatabaseService db, final Node node )
    {
        return new WorkerCommand<Void, Object>()
        {
            @Override
            public Object doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    node.getRelationships();
                    tx.success();
                }
                return null;
            }
        };
    }

    private WorkerCommand<Void, Object> setProperty( final GraphDatabaseService db, final Node node )
    {
        return new WorkerCommand<Void, Object>()
        {
            @Override
            public Object doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    node.setProperty( "name", "Dead locker" );
                    tx.success();
                }
                return null;
            }
        };
    }

    private DeadlockProneTransactionStateFactory testStateFactory;
    public final @Rule CleanupRule cleanup = new CleanupRule();

    @SuppressWarnings( "deprecation" )
    private ImpermanentGraphDatabase newDeadlockProneDb()
    {
        return new ImpermanentGraphDatabase()
        {
            @Override
            protected TransactionStateFactory createTransactionStateFactory()
            {
                return (testStateFactory = new DeadlockProneTransactionStateFactory( logging ));
            }
        };
    }

    private static class DeadlockProneTransactionStateFactory extends TransactionStateFactory
    {
        private DoubleLatch latch;

        DeadlockProneTransactionStateFactory( Logging logging )
        {
            super( logging );
        }

        public DoubleLatch installDeadlockLatch()
        {
            return this.latch = new DoubleLatch();
        }

        @Override
        public TransactionState create( javax.transaction.Transaction tx )
        {
            if ( latch != null )
            {
                return new DeadlockProneTransactionState(
                        locks.newClient(), nodeManager, logging, tx, txHook, txIdGenerator, latch );
            }
            return super.create( tx );
        }
    }

    private static class DeadlockProneTransactionState extends WritableTransactionState
    {
        private final DoubleLatch latch;

        public DeadlockProneTransactionState( Locks.Client lockManager, NodeManager nodeManager,
                Logging logging, javax.transaction.Transaction tx, RemoteTxHook txHook, TxIdGenerator txIdGenerator, DoubleLatch latch )
        {
            super( lockManager, nodeManager, txHook, txIdGenerator );
            this.latch = latch;
        }

        @Override
        public void applyChangesToCache()
        {
            latch.startAndAwaitFinish();
            super.applyChangesToCache();
        }
    }*/
}
