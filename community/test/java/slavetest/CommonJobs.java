package slavetest;

import java.io.Serializable;
import java.rmi.RemoteException;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.LockableNode;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;

public class CommonJobs
{
    public static final RelationshipType REL_TYPE = DynamicRelationshipType.withName( "HA_TEST" );
    public static final RelationshipType KNOWS = DynamicRelationshipType.withName( "KNOWS" );
    
    public static abstract class AbstractJob<T> implements Job<T>
    {
        protected Config getConfig( GraphDatabaseService db )
        {
            Config config = null;
            try
            {
                return (Config) db.getClass().getDeclaredMethod( "getConfig" ).invoke( db );
            }
            catch ( Exception e )
            {
                // Won't happen
                throw new RuntimeException( e );
            }
        }
    }
    
    public static abstract class TransactionalJob<T> extends AbstractJob<T>
    {
        public final T execute( GraphDatabaseService db ) throws RemoteException
        {
            Transaction tx = db.beginTx();
            try
            {
                return executeInTransaction( db, tx );
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

        protected abstract T executeInTransaction( GraphDatabaseService db, Transaction tx );
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
        
        protected Long executeInTransaction( GraphDatabaseService db, Transaction tx )
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
        protected Integer executeInTransaction( GraphDatabaseService db, Transaction tx )
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
        private final Job<Void> shutdownDispatcher;

        public CreateSubRefNodeMasterFailJob( Job<Void> shutdownDispatcher )
        {
            this.shutdownDispatcher = shutdownDispatcher;
        }

        public Serializable[] execute( GraphDatabaseService db ) throws RemoteException
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
                this.shutdownDispatcher.execute( db );
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
        
        protected Object executeInTransaction( GraphDatabaseService db, Transaction tx )
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
        protected Void executeInTransaction( GraphDatabaseService db, Transaction tx )
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
        
        public Boolean execute( GraphDatabaseService db )
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
    
    public static class ShutdownJvm implements Job<Void>
    {
        private final StandaloneDbCom jvm;

        public ShutdownJvm( StandaloneDbCom jvm )
        {
            this.jvm = jvm;
        }
        
        public Void execute( GraphDatabaseService db ) throws RemoteException
        {
            this.jvm.initiateShutdown();
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                // OK
            }
            return null;
        }
    }
    
    public static class DeleteNodeJob implements Job<Boolean>
    {
        private final long id;

        public DeleteNodeJob( long id )
        {
            this.id = id;
        }
        
        public Boolean execute( GraphDatabaseService db ) throws RemoteException
        {
            Transaction tx = db.beginTx();
            boolean successful = false;
            try
            {
                db.getNodeById( id ).delete();
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
        
        public Integer execute( GraphDatabaseService db )
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
        public Boolean execute( GraphDatabaseService db ) throws RemoteException
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
        @Override
        protected Long executeInTransaction( GraphDatabaseService db, Transaction tx )
        {
            Node node = db.createNode();
            tx.success();
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
        protected Boolean executeInTransaction( GraphDatabaseService db, Transaction tx )
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
    
    public static class CreateNodesJob extends TransactionalJob<Long[]>
    {
        private final int count;

        public CreateNodesJob( int count )
        {
            this.count = count;
        }
        
        @Override
        protected Long[] executeInTransaction( GraphDatabaseService db, Transaction tx )
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
        protected Boolean[] executeInTransaction( GraphDatabaseService db, Transaction tx )
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
        protected Boolean[] executeInTransaction( GraphDatabaseService db, Transaction tx )
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
    
    public static class PerformanceWriteLocksJob extends TransactionalJob<Void>
    {
        @Override
        protected Void executeInTransaction( GraphDatabaseService db, Transaction tx )
        {
            Config config = getConfig( db );
            LockManager lockManager = config.getLockManager();
            LockReleaser lockReleaser = config.getLockReleaser();
            for ( int i = 0; i < 10000; i++ )
            {
                Object resource = new LockableNode( i );
                lockManager.getWriteLock( resource );
                lockReleaser.addLockToTransaction( resource, LockType.WRITE );
            }
            return null;
        }
    }
    
    public static class PerformanceIdAllocationJob extends AbstractJob<Void>
    {
        public Void execute( GraphDatabaseService db )
        {
            Config config = getConfig( db );
            IdGenerator generator = config.getIdGeneratorFactory().get( IdType.NODE );
            for ( int i = 0; i < 100000; i++ )
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
        
        public Void execute( GraphDatabaseService db ) throws RemoteException
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
}
