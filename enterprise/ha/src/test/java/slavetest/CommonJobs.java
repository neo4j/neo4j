/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package slavetest;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.LockableNode;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;

public abstract class CommonJobs
{
    public static final RelationshipType REL_TYPE = DynamicRelationshipType.withName( "HA_TEST" );
    public static final RelationshipType KNOWS = DynamicRelationshipType.withName( "KNOWS" );

    public static abstract class AbstractJob<T> implements Job<T>
    {
    }

    public static abstract class TransactionalJob<T> extends AbstractJob<T>
    {
        public final T execute( GraphDatabaseAPI db ) throws RemoteException
        {
            Transaction tx = db.beginTx();
            try
            {
                return executeInTransaction( db, tx );
            }
            catch ( RuntimeException e )
            {
                e.printStackTrace( System.out );
                throw e;
            }
            finally
            {
                beforeFinish();
                tx.finish();
            }
        }

        protected void beforeFinish()
        {
        }

        protected abstract T executeInTransaction( GraphDatabaseAPI db, Transaction tx );
    }

    public static class CreateSubRefNodeJob extends TransactionalJob<Long>
    {
        private final String type;
        private final String key;
        private final Object value;

        public CreateSubRefNodeJob( String type, String key, Object value )
        {
            this.type = type;
            this.key = key;
            this.value = value;
        }

        @Override
        protected Long executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            Node node = db.createNode();
            Relationship rel = db.getReferenceNode().createRelationshipTo( node,
                    DynamicRelationshipType.withName( type ) );
            rel.setProperty( "something else", "Somewhat different" );
            if ( value != null )
            {
                node.setProperty( key, value );
            }
            tx.success();
            return node.getId();
        }
    }

    public static class CreateSubRefNodeWithRelCountJob extends TransactionalJob<Integer>
    {
        private final String type;
        private final String[] typesToAsk;

        public CreateSubRefNodeWithRelCountJob( String type,
                String... typesToAsk )
        {
            this.type = type;
            this.typesToAsk = typesToAsk;
        }

        @Override
        protected Integer executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            Node node = db.createNode();
            db.getReferenceNode().createRelationshipTo( node,
                    DynamicRelationshipType.withName( type ) );
            int counter = 0;
            for ( Relationship rel : db.getReferenceNode().getRelationships(
                    toRelationshipTypes( typesToAsk ) ) )
            {
                counter++;
            }
            tx.success();
            return counter;
        }
    }

    public static class CreateSubRefNodeMasterFailJob implements Job<Serializable[]>
    {
        private final CommonJobs.ShutdownDispatcher shutdownDispatcher;

        public CreateSubRefNodeMasterFailJob( CommonJobs.ShutdownDispatcher shutdownDispatcher )
        {
            this.shutdownDispatcher = shutdownDispatcher;
        }

        public Serializable[] execute( GraphDatabaseAPI db ) throws RemoteException
        {
            Transaction tx = db.beginTx();
            boolean successful = false;
            long nodeId = 0;
            try
            {
                Node node = db.createNode();
                nodeId = node.getId();
                db.getReferenceNode().createRelationshipTo( node, REL_TYPE );
                tx.success();
            }
            finally
            {
                this.shutdownDispatcher.doShutdown();
                try
                {
                    tx.finish();
                    successful = true;
                }
                catch ( RuntimeException e )
                {
                }
            }
            return new Serializable[] { successful, nodeId };
        }
    }

    public static class SetSubRefPropertyJob extends TransactionalJob<Object>
    {
        private final String key;
        private final Object value;

        public SetSubRefPropertyJob( String key, Object value )
        {
            this.key = key;
            this.value = value;
        }

        @Override
        protected Object executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            Node refNode = db.getReferenceNode();
            // To force it to pull updates
            refNode.removeProperty( "yoyoyoyo" );
            Node node = refNode.getSingleRelationship( REL_TYPE,
                    Direction.OUTGOING ).getEndNode();
            Object oldValue = node.getProperty( key, null );
            node.setProperty( key, value );
            tx.success();
            return oldValue;
        }
    }

    public static class CreateSomeEntitiesJob extends TransactionalJob<Void>
    {
        @Override
        protected Void executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            Node node1 = db.createNode();
            Relationship rel1 = db.getReferenceNode().createRelationshipTo( node1, REL_TYPE );
            node1.setProperty( "name", "Mattias" );
            rel1.setProperty( "something else", "Somewhat different" );

            Node node2 = db.createNode();
            Relationship rel2 = node1.createRelationshipTo( node2, REL_TYPE );
            node2.setProperty( "why o why", "Stuff" );
            rel2.setProperty( "random integer", "4" );
            tx.success();
            return null;
        }
    }

    public static class GetNodeByIdJob implements Job<Boolean>
    {
        private final long id;

        public GetNodeByIdJob( long id )
        {
            this.id = id;
        }

        public Boolean execute( GraphDatabaseAPI db )
        {
            try
            {
                db.getNodeById( id );
                return Boolean.TRUE;
            }
            catch ( NotFoundException e )
            {
                return Boolean.FALSE;
            }
        }
    }

    public interface ShutdownDispatcher extends Remote
    {
        void doShutdown() throws RemoteException;
    }

    public static class DeleteNodeJob implements Job<Boolean>
    {
        private final long id;

        public DeleteNodeJob( long id )
        {
            this.id = id;
        }

        public Boolean execute( GraphDatabaseAPI db ) throws RemoteException
        {
            Transaction tx = db.beginTx();
            boolean successful = false;
            try
            {
                Node node = db.getNodeById( id );
                node.delete();
                tx.success();
            }
            finally
            {
                try
                {
                    tx.finish();
                    successful = true;
                }
                catch ( RuntimeException e )
                {
                }
            }
            return successful;
        }
    }

    public static class GetRelationshipCountJob implements Job<Integer>
    {
        private final String[] types;

        public GetRelationshipCountJob( String... types )
        {
            this.types = types;
        }

        public Integer execute( GraphDatabaseAPI db )
        {
            int counter = 0;
            for ( Relationship rel : db.getReferenceNode().getRelationships(
                    toRelationshipTypes( types ) ) )
            {
                counter++;
            }
            return counter;
        }
    }

    private static RelationshipType[] toRelationshipTypes( String... names )
    {
        RelationshipType[] types = new RelationshipType[names.length];
        for ( int i = 0; i < names.length; i++ )
        {
            types[i] = DynamicRelationshipType.withName( names[i] );
        }
        return types;
    }

    public static class CreateNodeOutsideOfTxJob implements Job<Boolean>
    {
        public Boolean execute( GraphDatabaseAPI db ) throws RemoteException
        {
            try
            {
                db.getReferenceNode().createRelationshipTo( db.createNode(), REL_TYPE );
                return Boolean.TRUE;
            }
            catch ( Exception e )
            {
                return Boolean.FALSE;
            }
        }
    }

    public static class CreateNodeJob extends TransactionalJob<Long>
    {
        private final boolean beSuccessful;

        public CreateNodeJob()
        {
            this( true );
        }

        public CreateNodeJob( boolean beSuccessful )
        {
            this.beSuccessful = beSuccessful;
        }

        @Override
        protected Long executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            Node node = db.createNode();
            if ( beSuccessful ) tx.success();
            return node.getId();
        }
    }

    public static class SetNodePropertyJob extends TransactionalJob<Boolean>
    {
        private final long id;
        private final String key;
        private final Object value;

        public SetNodePropertyJob( long id, String key, Object value )
        {
            this.id = id;
            this.key = key;
            this.value = value;
        }

        @Override
        protected Boolean executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            try
            {
                db.getNodeById( id ).setProperty( key, value );
                tx.success();
                return Boolean.TRUE;
            }
            catch ( Exception e )
            {
                return Boolean.FALSE;
            }
        }
    }

    public static class SetNodePropertyWithThrowJob implements Job<Void>
    {
        private final long id;
        private final String key;
        private final Object value;
        private final long firstId;

        public SetNodePropertyWithThrowJob( long firstId, long id, String key, Object value )
        {
            this.firstId = firstId;
            this.id = id;
            this.key = key;
            this.value = value;
        }

        @Override
        public Void execute( GraphDatabaseAPI db )
        {
            Transaction tx = db.beginTx();
            try
            {
                tx.acquireWriteLock( db.getNodeById( firstId ) );
                db.getNodeById( id ).setProperty( key, value );
                tx.success();
                return null;
            }
            finally
            {
                tx.finish();
            }
        }
    }
    
    public static class CreateNodesJob extends TransactionalJob<Long[]>
    {
        private final int count;

        public CreateNodesJob( int count )
        {
            this.count = count;
        }

        @Override
        protected Long[] executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            Long[] result = new Long[count];
            for ( int i = 0; i < count; i++ )
            {
                result[i] = db.createNode().getId();
            }
            tx.success();
            return result;
        }
    }

    public static class Worker1Job extends TransactionalJob<Boolean[]>
    {
        private final long node1;
        private final long node2;
        private final Fetcher<DoubleLatch> fetcher;

        public Worker1Job( long node1, long node2, Fetcher<DoubleLatch> fetcher )
        {
            this.node1 = node1;
            this.node2 = node2;
            this.fetcher = fetcher;
        }

        @Override
        protected Boolean[] executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            boolean success = false;
            boolean deadlock = false;
            try
            {
                DoubleLatch latch = fetcher.fetch();
                db.getNodeById( node1 ).setProperty( "1", "T1 1" );
                latch.countDownSecond();
                latch.awaitFirst();
                db.getNodeById( node2 ).removeProperty( "2" );
                db.getNodeById( node1 ).removeProperty( "1" );
                tx.success();
                success = true;
            }
            catch ( DeadlockDetectedException e )
            {
                deadlock = true;
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            return new Boolean[] { success, deadlock };
        }
    }

    public static class Worker2Job extends TransactionalJob<Boolean[]>
    {
        private final long node1;
        private final long node2;
        private final Fetcher<DoubleLatch> fetcher;

        public Worker2Job( long node1, long node2, Fetcher<DoubleLatch> fetcher )
        {
            this.node1 = node1;
            this.node2 = node2;
            this.fetcher = fetcher;
        }

        @Override
        protected Boolean[] executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            boolean success = false;
            boolean deadlock = false;
            try
            {
                DoubleLatch latch = fetcher.fetch();
                db.getNodeById( node2 ).setProperty( "2", "T2 2" );
                latch.countDownFirst();
                latch.awaitSecond();
                db.getNodeById( node1 ).setProperty( "1", "T2 2" );
                tx.success();
                success = true;
            }
            catch ( DeadlockDetectedException e )
            {
                deadlock = true;
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            return new Boolean[] { success, deadlock };
        }
    }

    public static class SetGraphProperty1 extends TransactionalJob<Boolean[]>
    {
        private final Fetcher<DoubleLatch> fetcher;
        private final long node;

        public SetGraphProperty1( long node, Fetcher<DoubleLatch> fetcher )
        {
            this.node = node;
            this.fetcher = fetcher;
        }

        @Override
        protected Boolean[] executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            boolean success = false;
            boolean deadlock = false;
            try
            {
                PropertyContainer properties = db.getNodeManager().getGraphProperties();
                DoubleLatch latch = fetcher.fetch();
                db.getNodeById( node ).setProperty( "1", "T1 1" );
                latch.countDownSecond();
                latch.awaitFirst();
                properties.removeProperty( "2" );
                properties.removeProperty( "1" );
                tx.success();
                success = true;
            }
            catch ( DeadlockDetectedException e )
            {
                deadlock = true;
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            return new Boolean[] { success, deadlock };
        }
    }

    public static class SetGraphPropertyJob extends TransactionalJob<Void>
    {
        private final String key;
        private final Object value;

        public SetGraphPropertyJob( String key, Object value )
        {
            this.key = key;
            this.value = value;
        }

        @Override
        protected Void executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            db.getNodeManager().getGraphProperties().setProperty( key, value );
            tx.success();
            return null;
        }
    }

    public static class GetGraphProperty extends AbstractJob<Object>
    {
        private final String key;

        public GetGraphProperty( String key )
        {
            this.key = key;
        }

        @Override
        public Object execute( GraphDatabaseAPI db ) throws RemoteException
        {
            GraphProperties properties = db.getNodeManager().getGraphProperties();
            return properties.getProperty( key );
        }
    }

    public static class SetGraphProperty2 extends TransactionalJob<Boolean[]>
    {
        private final Fetcher<DoubleLatch> fetcher;
        private final long node;

        public SetGraphProperty2( long node, Fetcher<DoubleLatch> fetcher )
        {
            this.node = node;
            this.fetcher = fetcher;
        }

        @Override
        protected Boolean[] executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            boolean success = false;
            boolean deadlock = false;
            try
            {
                PropertyContainer properties = db.getNodeManager().getGraphProperties();
                DoubleLatch latch = fetcher.fetch();
                properties.setProperty( "2", "T2 2" );
                latch.countDownFirst();
                latch.awaitSecond();
                db.getNodeById( node ).setProperty( "1", "T2 2" );
                tx.success();
                success = true;
            }
            catch ( DeadlockDetectedException e )
            {
                deadlock = true;
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            return new Boolean[] { success, deadlock };
        }
    }

    public static class PerformanceAcquireWriteLocksJob extends TransactionalJob<Void>
    {
        private final int amount;

        public PerformanceAcquireWriteLocksJob( int amount )
        {
            this.amount = amount;
        }

        @Override
        protected Void executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            LockManager lockManager = db.getLockManager();
            TransactionState state = ((AbstractTransactionManager)db.getTxManager()).getTransactionState();
            for ( int i = 0; i < amount; i++ )
            {
                Object resource = new LockableNode( i );
                lockManager.getWriteLock( resource );
                state.addLockToTransaction( lockManager, resource, LockType.WRITE );
            }
            return null;
        }
    }

    public static class PerformanceIdAllocationJob extends AbstractJob<Void>
    {
        private final int count;

        public PerformanceIdAllocationJob( int count )
        {
            this.count = count;
        }

        public Void execute( GraphDatabaseAPI db )
        {
            IdGenerator generator = db.getIdGeneratorFactory().get( IdType.NODE );
            for ( int i = 0; i < count; i++ )
            {
                generator.nextId();
            }
            return null;
        }
    }

    public static class PerformanceCreateNodesJob extends AbstractJob<Void>
    {
        private final int numTx;
        private final int numNodesInEach;

        public PerformanceCreateNodesJob( int numTx, int numNodesInEach )
        {
            this.numTx = numTx;
            this.numNodesInEach = numNodesInEach;
        }

        public Void execute( GraphDatabaseAPI db ) throws RemoteException
        {
            for ( int i = 0; i < numTx; i++ )
            {
                Transaction tx = db.beginTx();
                try
                {
                    for ( int ii = 0; ii < numNodesInEach; ii++ )
                    {
                        db.createNode();
                    }
                    tx.success();
                }
                finally
                {
                    tx.finish();
                }
            }
            return null;
        }
    }

    public static class CreateNodeAndIndexJob extends TransactionalJob<Long>
    {
        private final String key;
        private final Object value;

        public CreateNodeAndIndexJob( String key, Object value )
        {
            this.key = key;
            this.value = value;
        }

        @Override
        protected Long executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            Node node = db.createNode();
            node.setProperty( key, value );
            tx.success();
            return node.getId();
        }
    }

    public static class CreateNodeAndNewIndexJob extends TransactionalJob<Long>
    {
        private final String key;
        private final Object value;
        private final String indexName;
        private final String key2;
        private final Object value2;

        public CreateNodeAndNewIndexJob( String indexName,
                String key, Object value, String key2, Object value2 )
        {
            this.indexName = indexName;
            this.key = key;
            this.value = value;
            this.key2 = key2;
            this.value2 = value2;
        }

        @Override
        protected Long executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            Node node = db.createNode();
            node.setProperty( key, value );
            node.setProperty( key2, value2 );
            Index<Node> index = db.index().forNodes( indexName );
            index.add( node, key, value );
            index.add( node, key2, value2 );
            tx.success();
            return node.getId();
        }
    }

    public static class AddIndex extends TransactionalJob<Void>
    {
        private final Map<String, Object> properties;
        private final long nodeId;

        public AddIndex( long nodeId, Map<String, Object> properties )
        {
            this.nodeId = nodeId;
            this.properties = properties;
        }

        @Override
        protected Void executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
//            IndexService index = ((HighlyAvailableGraphDatabase) db).getIndexService();
            Node node = db.getNodeById( nodeId );
            for ( Map.Entry<String, Object> entry : properties.entrySet() )
            {
//                index.index( node, entry.getKey(), entry.getValue() );
            }
            tx.success();
            return null;
        }
    }

    public static class LargeTransactionJob extends AbstractJob<Void>
    {
        private final int txSizeMb;
        private final int numTxs;

        public LargeTransactionJob( int txSizeMb, int numTxs )
        {
            this.txSizeMb = txSizeMb;
            this.numTxs = numTxs;
        }

        public Void execute( GraphDatabaseAPI db ) throws RemoteException
        {
            byte[] largeArray = new byte[1*1024*1021]; /* 1021 So that it doesn't align with block size in BlockLogBuffer and all that :) */
            for ( int t = 0; t < numTxs; t++ )
            {
                Transaction tx = db.beginTx();
                try
                {
                    for ( int i = 0; i < largeArray.length; i++ )
                    {
                        largeArray[i] = (byte) (i % 256);
                    }
                    for ( int i = 0; i < txSizeMb; i++ )
                    {
                        Node node = db.createNode();
                        node.setProperty( "data", largeArray );
                    }
                    tx.success();
                }
                finally
                {
                    tx.finish();
                }
            }
            return null;
        }
    }

    public static class CreateNodeNoCommit extends AbstractJob<Void>
    {
        private Transaction tx;

        public Void execute( GraphDatabaseAPI db ) throws RemoteException
        {
            tx = db.beginTx();
            db.createNode();
            return null;
        }

        public void rollback()
        {
            tx.finish();
        }
    }

    public static class HoldLongLock extends TransactionalJob<Void>
    {
        private final long nodeId;
        private final Fetcher<DoubleLatch> latchFetcher;

        public HoldLongLock( long nodeId, Fetcher<DoubleLatch> latchFetcher )
        {
            this.nodeId = nodeId;
            this.latchFetcher = latchFetcher;
        }

        @Override
        protected Void executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            DoubleLatch latch = latchFetcher.fetch();
            Node node = db.getNodeById( nodeId );
            node.removeProperty( "something something" );
            try
            {
                latch.countDownFirst();
                latch.awaitSecond();
            }
            catch ( RemoteException e )
            {
                throw new RuntimeException( e );
            }
            tx.success();
            return null;
        }
    }

    public static class AcquireNodeLockAndReleaseManually extends TransactionalJob<Void>
    {
        private final long nodeId;
        private final Fetcher<DoubleLatch> latchFetcher;

        public AcquireNodeLockAndReleaseManually( long nodeId, Fetcher<DoubleLatch> latchFetcher )
        {
            this.nodeId = nodeId;
            this.latchFetcher = latchFetcher;
        }

        @Override
        protected Void executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            Lock lock = tx.acquireWriteLock( db.getNodeById( nodeId ) );
            lock.release();
            try
            {
                latchFetcher.fetch().awaitFirst();
            }
            catch ( RemoteException e )
            {
                throw new RuntimeException( e );
            }
            return null;
        }
    }

    public static class IndexPutIfAbsentPartOne extends TransactionalJob<Node>
    {
        final long nodeId;
        final String key;
        final Object value;
        final Fetcher<DoubleLatch> latchFetcher;
        final String index;

        public IndexPutIfAbsentPartOne( long nodeId, String index, String key, Object value, Fetcher<DoubleLatch> latchFetcher )
        {
            this.nodeId = nodeId;
            this.index = index;
            this.key = key;
            this.value = value;
            this.latchFetcher = latchFetcher;
        }

        @Override
        protected Node executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            DoubleLatch latch = latchFetcher.fetch();
            Node result = db.index().forNodes( index ).putIfAbsent( db.getNodeById( nodeId ), key, value );
            try
            {
                latch.countDownFirst();
                latch.awaitSecond();
            }
            catch ( RemoteException e )
            {
                throw new RuntimeException( e );
            }
            tx.success();
            return result;
        }
    }

    public static class IndexPutIfAbsentPartTwo extends IndexPutIfAbsentPartOne
    {
        public IndexPutIfAbsentPartTwo( long nodeId, String index, String key, Object value,
                Fetcher<DoubleLatch> latchFetcher )
        {
            super( nodeId, index, key, value, latchFetcher );
        }

        @Override
        protected Node executeInTransaction( GraphDatabaseAPI db, Transaction tx )
        {
            DoubleLatch latch = latchFetcher.fetch();
            try
            {
                latch.awaitFirst();
                latch.countDownSecond();
            }
            catch ( RemoteException e )
            {
                throw new RuntimeException( e );
            }
            Node result = db.index().forNodes( index ).putIfAbsent( db.getNodeById( nodeId ), key, value );
            tx.success();
            return result;
        }
    }
}
