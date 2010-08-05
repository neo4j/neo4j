package slavetest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

public class BasicHaTesting
{
    private static final RelationshipType REL_TYPE = DynamicRelationshipType.withName( "HA_TEST" );
    private static final File PARENT_PATH = new File( "target/dbs" );
    private static final File MASTER_PATH = new File( PARENT_PATH, "master" );
    private static final File SLAVE_PATH = new File( PARENT_PATH, "slave" );
    private FakeMaster master;
    private List<GraphDatabaseService> haDbs;
    private static final Predicate<Integer> ALL = new Predicate<Integer>()
    {
        public boolean accept( Integer item )
        {
            return true;
        }
    };
    private Predicate<Integer> verificationFilter = ALL;
    
    private void initializeDbs( int numSlaves )
    {
        try
        {
            FileUtils.deleteDirectory( PARENT_PATH );
            GraphDatabaseService masterDb =
                    new EmbeddedGraphDatabase( MASTER_PATH.getAbsolutePath() );
            masterDb.shutdown();
            for ( int i = 0; i < numSlaves; i++ )
            {
                FileUtils.copyDirectory( MASTER_PATH, slavePath( i ) );
            }
            haDbs = new ArrayList<GraphDatabaseService>();
            master = new FakeMaster( MASTER_PATH.getAbsolutePath() );
            for ( int i = 0; i < numSlaves; i++ )
            {
                File slavePath = slavePath( i );
                FakeBroker broker = new FakeBroker( master, i ); 
                GraphDatabaseService db = new HighlyAvailableGraphDatabase(
                        slavePath.getAbsolutePath(), new HashMap<String, String>(), broker );
                haDbs.add( db );
                broker.setSlave( db );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private static File slavePath( int num )
    {
        return new File( SLAVE_PATH, "" + num );
    }
    
    private void shutdownDbs()
    {
        for ( GraphDatabaseService haDb : haDbs )
        {
            haDb.shutdown();
        }
        master.getGraphDb().shutdown();
    }

    private void verify( GraphDatabaseService refDb, GraphDatabaseService... dbs )
    {
        for ( Node node : refDb.getAllNodes() )
        {
            int counter = 0;
            for ( GraphDatabaseService otherDb : dbs )
            {
                if ( verificationFilter.accept( counter++ ) )
                {
                    Node otherNode = otherDb.getNodeById( node.getId() );
                    verifyNode( node, otherNode, otherDb );
                }
            }
        }
    }
    
    private static String tab( int times )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < times; i++ )
        {
            builder.append( "\t" );
        }
        return builder.toString();
    }

    private static void verifyNode( Node node, Node otherNode, GraphDatabaseService otherDb )
    {
        System.out.println( "[" + node + "]" );
        verifyProperties( node, otherNode );
        Set<Long> otherRelIds = new HashSet<Long>();
        for ( Relationship otherRel : otherNode.getRelationships( Direction.OUTGOING ) )
        {
            otherRelIds.add( otherRel.getId() );
        }
        
        for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
        {
            Relationship otherRel = otherDb.getRelationshipById( rel.getId() );
            System.out.println( tab( 1 ) + rel.getStartNode() + " --[" +
                    rel.getType().name() + "," + rel.getId() + "]-> " + rel.getEndNode() );
            verifyProperties( rel, otherRel );
            if ( rel.getStartNode().getId() != otherRel.getStartNode().getId() )
            {
                throw new RuntimeException( "Start node differs on " + rel );
            }
            if ( rel.getEndNode().getId() != otherRel.getEndNode().getId() )
            {
                throw new RuntimeException( "End node differs on " + rel );
            }
            if ( !rel.getType().name().equals( otherRel.getType().name() ) )
            {
                throw new RuntimeException( "Type differs on " + rel );
            }
            otherRelIds.remove( rel.getId() );
        }
        
        if ( !otherRelIds.isEmpty() )
        {
            throw new RuntimeException( "Other node " + otherNode + " has more relationships " +
                    otherRelIds );
        }
    }

    private static void verifyProperties( PropertyContainer entity, PropertyContainer otherEntity )
    {
        Set<String> otherKeys = IteratorUtil.addToCollection(
                otherEntity.getPropertyKeys().iterator(), new HashSet<String>() );
        for ( String key : entity.getPropertyKeys() )
        {
            Object value1 = entity.getProperty( key );
            Object value2 = otherEntity.getProperty( key );
            System.out.println( tab( entity instanceof Relationship ? 2 : 1 ) +
                    "*" + key + "=" + value1 );
            if ( !value1.equals( value2 ) )
            {
                throw new RuntimeException( entity + " not equals property '" + key + "': " +
                        value1 + ", " + value2 );
            }
            otherKeys.remove( key );
        }
        if ( !otherKeys.isEmpty() )
        {
            throw new RuntimeException( "Other node " + otherEntity + " has more properties: " +
                    otherKeys );
        }
    }
    
    @After
    public void verifyAndShutdownDbs()
    {
        System.out.println( "ONLINE VERIFICATION" );
        verify( master.getGraphDb(), haDbs.toArray( new GraphDatabaseService[haDbs.size()] ) );
        shutdownDbs();
        
        GraphDatabaseService masterOfflineDb =
                new EmbeddedGraphDatabase( MASTER_PATH.getAbsolutePath() );
        GraphDatabaseService[] slaveOfflineDbs = new GraphDatabaseService[haDbs.size()];
        for ( int i = 0; i < haDbs.size(); i++ )
        {
            slaveOfflineDbs[i] = new EmbeddedGraphDatabase( slavePath( i ).getAbsolutePath() );
        }
        System.out.println( "OFFLINE VERIFICATION" );
        verify( masterOfflineDb, slaveOfflineDbs );
        masterOfflineDb.shutdown();
        for ( GraphDatabaseService db : slaveOfflineDbs )
        {
            db.shutdown();
        }
    }
    
    private void pullUpdates()
    {
        for ( GraphDatabaseService db : haDbs )
        {
            ((HighlyAvailableGraphDatabase) db).pullUpdates();
        }
    }
    
    public static <T> void assertCollection( Collection<T> collection,
            T... expectedItems )
    {
        String collectionString = join( ", ", collection.toArray() );
        assertEquals( collectionString, expectedItems.length,
                collection.size() );
        for ( T item : expectedItems )
        {
            assertTrue( collection.contains( item ) );
        }
    }
    
    public static <T> String join( String delimiter, T... items )
    {
        StringBuffer buffer = new StringBuffer();
        for ( T item : items )
        {
            if ( buffer.length() > 0 )
            {
                buffer.append( delimiter );
            }
            buffer.append( item.toString() );
        }
        return buffer.toString();
    }
    
//    private static class GrabWriteLockJob implements Job
//    {
//        private final Node node;
//
//        GrabWriteLockJob( Node node )
//        {
//            this.node = node;
//        }
//        
//        public String execute( WaitingWorker worker )
//        {
//            try
//            {
//                node.removeProperty( "skdlsdksldk" );
//                return "W " + node.getId();
//            }
//            catch ( DeadlockDetectedException e )
//            {
//                return "DL " + node.getId();
//            }
//        }
//    }
//    
//    private static class RestartTxJob implements Job
//    {
//        private final boolean successful;
//
//        RestartTxJob( boolean successful )
//        {
//            this.successful = successful;
//        }
//        
//        public String execute( WaitingWorker worker )
//        {
//            worker.endTx( successful );
//            worker.beginTx();
//            return null;
//        }
//    }
//    
//    private static class SetPropertyJob implements Job
//    {
//        private final Node node;
//
//        SetPropertyJob( Node node )
//        {
//            this.node = node;
//        }
//        
//        public String execute( WaitingWorker worker )
//        {
//            node.setProperty( "yo", "ya" );
//            return "SP " + node.getId();
//        }
//    }
//    
//    private static class DeleteNodeAndRelsJob implements Job
//    {
//        private final Node node;
//
//        DeleteNodeAndRelsJob( Node node )
//        {
//            this.node = node;
//        }
//        
//        public String execute( WaitingWorker worker )
//        {
//            for ( Relationship rel : node.getRelationships() )
//            {
//                rel.delete();
//            }
//            node.delete();
//            return "DNR " + node.getId();
//        }
//    }
//    
//    private static interface Job
//    {
//        String execute( WaitingWorker worker );
//    }
//    
//    private static class WaitingWorker extends Thread
//    {
//        private final Queue<Job> jobs = new LinkedList<Job>();
//        private final Queue<String> results;
//        private final AtomicBoolean halted = new AtomicBoolean();
//        private final GraphDatabaseService db;
//        private Transaction tx;
//        
//        WaitingWorker( String name, GraphDatabaseService db, Queue<String> results )
//        {
//            super( name );
//            this.db = db;
//            this.results = results;
//            start();
//        }
//        
//        private void beginTx()
//        {
//            tx = db.beginTx();
//        }
//        
//        private void endTx( boolean success )
//        {
//            if ( success )
//            {
//                tx.success();
//            }
//            tx.finish();
//            tx = null;
//        }
//        
//        public synchronized void add( Job job, boolean waitForCompletion )
//        {
//            synchronized ( jobs )
//            {
//                this.jobs.add( job );
//            }
//            if ( waitForCompletion )
//            {
//                try
//                {
//                    Thread.sleep( 50 );
//                }
//                catch ( InterruptedException e )
//                {
//                    Thread.interrupted();
//                }
//            }
//        }
//        
//        @Override
//        public void run()
//        {
//            beginTx();
//            while ( !halted.get() || hasMoreJobs() )
//            {
//                Job job = null;
//                synchronized ( jobs )
//                {
//                    job = jobs.poll();
//                }
//                if ( job == null )
//                {
//                    try
//                    {
//                        Thread.sleep( 1 );
//                    }
//                    catch ( InterruptedException e )
//                    {
//                        Thread.interrupted();
//                    }
//                    continue;
//                }
//                
//                String result = job.execute( this );
//                synchronized ( results )
//                {
//                    if ( result != null )
//                    {
//                        results.add( getName() + ":" + result );
//                    }
//                }
//            }
//            endTx( true );
//        }
//        
//        private synchronized boolean hasMoreJobs()
//        {
//            return !jobs.isEmpty();
//        }
//
//        public synchronized void halt()
//        {
//            this.halted.set( true );
//            notify();
//        }
//    }
    
    @Test
    public void slaveCreateNode() throws Exception
    {
        initializeDbs( 1 );
        GraphDatabaseService haDb = haDbs.get( 0 );
        Transaction tx = haDb.beginTx();
        try
        {
            Node node1 = haDb.createNode();
            Relationship rel1 = haDb.getReferenceNode().createRelationshipTo( node1, REL_TYPE );
            node1.setProperty( "name", "Mattias" );
            rel1.setProperty( "something else", "Somewhat different" );
            
            Node node2 = haDb.createNode();
            Relationship rel2 = node1.createRelationshipTo( node2, REL_TYPE );
            node2.setProperty( "why o why", "Stuff" );
            rel2.setProperty( "random integer", "4" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
    
    @Test
    public void testMultipleSlaves()
    {
        initializeDbs( 3 );
        GraphDatabaseService db1 = haDbs.get( 0 );
        GraphDatabaseService db2 = haDbs.get( 1 );
        GraphDatabaseService db3 = haDbs.get( 2 );
        
        // Create a node (with a relationship from ref node) on db1
        Transaction tx = db1.beginTx();
        try
        {
            Node node = db1.createNode();
            db1.getReferenceNode().createRelationshipTo( node, REL_TYPE );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        
        // Get that node on db2 and set a property on it
        tx = db2.beginTx();
        try
        {
            db2.getReferenceNode().removeProperty( "bög" );
            Node node = db2.getReferenceNode().getSingleRelationship(
                    REL_TYPE, Direction.OUTGOING ).getEndNode();
            node.setProperty( "name", "Hello" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        
        // See if db1 and 3 can see it the same way as 2
        ((HighlyAvailableGraphDatabase) db1).pullUpdates();
        ((HighlyAvailableGraphDatabase) db3).pullUpdates();
    }
    
    @Test
    public void testMasterFailure()
    {
        initializeDbs( 1 );
        GraphDatabaseService haDb = haDbs.get( 0 );
        Transaction tx = haDb.beginTx();
        Node node1;
        try
        {
            node1 = haDb.createNode();
            Relationship rel1 = haDb.getReferenceNode().createRelationshipTo( node1, REL_TYPE );
            node1.setProperty( "name", "Mattias" );
            rel1.setProperty( "something else", "Somewhat different" );
            tx.success();
        }
        finally
        {
            master.getGraphDb().shutdown();
            try
            {
                tx.finish();
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
            }
        }
        master = new FakeMaster( MASTER_PATH.getAbsolutePath() );
        try
        {
            haDb.getNodeById( node1.getId() );
            fail( "Node should not exist");
        }
        catch ( NotFoundException e )
        { // good
        }
    }

    @Test
    public void testSlaveConstrainViolation()
    {
        initializeDbs( 1 );
        GraphDatabaseService haDb = haDbs.get( 0 );
        Transaction tx = haDb.beginTx();
        Node node1;
        try
        {
            node1 = haDb.createNode();
            Relationship rel1 = haDb.getReferenceNode().createRelationshipTo( node1, REL_TYPE );
            node1.setProperty( "name", "Mattias" );
            rel1.setProperty( "something else", "Somewhat different" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        tx = haDb.beginTx();
        try
        {
           node1.delete();
           tx.success();
        }
        finally
        {
            try
            {
                tx.finish();
                fail( "Should throw exception" );
            }
            catch ( Throwable t )
            { // good
                t.printStackTrace();
            }
        }
    }
    
    @Test
    public void testMasterConstrainViolation()
    {
        initializeDbs( 1 );
        GraphDatabaseService haDb = haDbs.get( 0 );
        Transaction tx = haDb.beginTx();
        Node node1;
        try
        {
            node1 = haDb.createNode();
            Relationship rel1 = haDb.getReferenceNode().createRelationshipTo( node1, REL_TYPE );
            node1.setProperty( "name", "Mattias" );
            rel1.setProperty( "something else", "Somewhat different" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        tx = master.getGraphDb().beginTx();
        try
        {
           master.getGraphDb().getNodeById( node1.getId() ).delete();
           tx.success();
        }
        finally
        {
            try
            {
                tx.finish();
                fail( "Should throw exception" );
            }
            catch ( Throwable t )
            { // good
                t.printStackTrace();
            }
        }
        ((HighlyAvailableGraphDatabase) haDb).pullUpdates();
        assertTrue( haDb.getNodeById( node1.getId() ) != null );
    }
    
//    @Test
//    public void testLockingOnSingleSlave() throws Exception
//    {
//        initializeDbs( 1 );
//        GraphDatabaseService slave = haDbs.get( 0 );
//        
//        Transaction tx = master.getGraphDb().beginTx();
//        try
//        {
//            Node node1 = master.getGraphDb().createNode();
//            master.getGraphDb().getReferenceNode().createRelationshipTo( node1, REL_TYPE );
//            Node node2 = master.getGraphDb().createNode();
//            node1.createRelationshipTo( node2, REL_TYPE );
//            tx.success();
//        }
//        finally
//        {
//            tx.finish();
//        }
//        
//        ((HighlyAvailableGraphDatabase) slave).pullUpdates();
//        Node node1 = slave.getReferenceNode().getSingleRelationship(
//                REL_TYPE, Direction.OUTGOING ).getEndNode();
//        Node node2 = node1.getSingleRelationship( REL_TYPE, Direction.OUTGOING ).getEndNode();
//        
//        Queue<String> results = new LinkedList<String>();
//        WaitingWorker worker1 = new WaitingWorker( "1", slave, results );
//        WaitingWorker worker2 = new WaitingWorker( "2", slave, results );
//        WaitingWorker masterWorker = new WaitingWorker( "M", master.getGraphDb(), results );
//        worker1.add( new GrabWriteLockJob( node1 ), true );
//        worker2.add( new GrabWriteLockJob( node2 ), true );
//        worker1.add( new GrabWriteLockJob( node2 ), true );
//        worker2.add( new GrabWriteLockJob( node1 ), true );
//        worker2.add( new RestartTxJob( true ), true );
//        
//        Thread.sleep( 1000 );
//        
//        worker1.halt();
//        worker2.halt();
//        masterWorker.halt();
//        
//        System.out.println( results );
//        assertCollection( results, "1:W " + node1.getId(),
//                "2:W " + node2.getId(), "2:DL " + node1.getId() );
//    }
//    
//    @Test
//    public void testLockingOnMyltipleSlaves()
//    {
//        initializeDbs( 2 );
//        GraphDatabaseService slave1 = haDbs.get( 0 );
//        GraphDatabaseService slave2 = haDbs.get( 1 );
//        
//        Transaction tx = master.getGraphDb().beginTx();
//        try
//        {
//            Node node1 = master.getGraphDb().createNode();
//            master.getGraphDb().getReferenceNode().createRelationshipTo( node1, REL_TYPE );
//            Node node2 = master.getGraphDb().createNode();
//            node1.createRelationshipTo( node2, REL_TYPE );
//            tx.success();
//        }
//        finally
//        {
//            tx.finish();
//        }
//        
//        ((HighlyAvailableGraphDatabase) slave1).pullUpdates();
//        ((HighlyAvailableGraphDatabase) slave2).pullUpdates();
//        Node slave1Node1 = slave1.getReferenceNode().getSingleRelationship(
//                REL_TYPE, Direction.OUTGOING ).getEndNode();
//        Node slave1Node2 = slave1Node1.getSingleRelationship( REL_TYPE, Direction.OUTGOING ).getEndNode();
//        Node slave2Node1 = slave2.getReferenceNode().getSingleRelationship(
//                REL_TYPE, Direction.OUTGOING ).getEndNode();
//        Node slave2Node2 = slave2Node1.getSingleRelationship( REL_TYPE, Direction.OUTGOING ).getEndNode();
//        
//        Queue<String> results = new LinkedList<String>();
//        WaitingWorker worker1 = new WaitingWorker( "1", slave1, results );
//        WaitingWorker worker2 = new WaitingWorker( "2", slave2, results );
//        worker1.add( new GrabWriteLockJob( slave1Node1 ), true );
//        worker2.add( new GrabWriteLockJob( slave2Node2 ), true );
//        worker1.add( new GrabWriteLockJob( slave1Node2 ), true );
//        worker2.add( new GrabWriteLockJob( slave2Node1 ), true );
//        
//        worker1.halt();
//        worker2.halt();
//        
//        System.out.println( results );
//        assertCollection( results, "1:W " + slave1Node1.getId(),
//                "2:W " + slave2Node2.getId(), "2:DL " + slave2Node1.getId() );
//    }
    
//    @Test
//    public void testRollbackTx() throws Exception
//    {
//        initializeDbs( 2 );
//        GraphDatabaseService slave1 = haDbs.get( 0 );
//        GraphDatabaseService slave2 = haDbs.get( 1 );
//        
//        Transaction tx = master.getGraphDb().beginTx();
//        try
//        {
//            Node node1 = master.getGraphDb().createNode();
//            master.getGraphDb().getReferenceNode().createRelationshipTo( node1, REL_TYPE );
//            Node node2 = master.getGraphDb().createNode();
//            node1.createRelationshipTo( node2, REL_TYPE );
//            tx.success();
//        }
//        finally
//        {
//            tx.finish();
//        }
//        
//        pullUpdates();
//        Node slave1Node1 = slave1.getReferenceNode().getSingleRelationship(
//                REL_TYPE, Direction.OUTGOING ).getEndNode();
//        Node slave1Node2 = slave1Node1.getSingleRelationship(
//                REL_TYPE, Direction.OUTGOING ).getEndNode();
//        Node slave2Node1 = slave2.getReferenceNode().getSingleRelationship(
//                REL_TYPE, Direction.OUTGOING ).getEndNode();
//        Node slave2Node2 = slave2Node1.getSingleRelationship(
//                REL_TYPE, Direction.OUTGOING ).getEndNode();
//
//        Queue<String> results = new LinkedList<String>();
//        WaitingWorker worker1 = new WaitingWorker( "1", slave1, results );
//        WaitingWorker worker2 = new WaitingWorker( "2", slave2, results );
//        worker1.add( new SetPropertyJob( slave1Node2 ), true );
//        worker2.add( new DeleteNodeAndRelsJob( slave2Node2 ), true );
//        worker2.add( new RestartTxJob( false ), true );
//        worker1.add( new RestartTxJob( true ), true );
//        worker2.halt();
//        worker1.halt();
//        Thread.sleep( 1000 );
//        
//        System.out.println( results );
//        
//        pullUpdates();
//    }

    @Test
    public void testDeadlock() throws Exception
    {
        initializeDbs( 2 );
        GraphDatabaseService haDb1 = haDbs.get( 0 );
        GraphDatabaseService haDb2 = haDbs.get( 1 );
        GraphDatabaseService mDb = master.getGraphDb();
        
        Transaction tx = mDb.beginTx();
        Node node1, node2;
        try
        {
            node1 = mDb.createNode();
            node2 = mDb.createNode();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        ((HighlyAvailableGraphDatabase) haDb1).pullUpdates();
        ((HighlyAvailableGraphDatabase) haDb2).pullUpdates();
        // verify( mDb, new GraphDatabaseService[] {haDb1, haDb2} );
        CountDownLatch barrier1 = new CountDownLatch( 1 );
        CountDownLatch barrier2 = new CountDownLatch( 1 );
        Worker1 w1 = new Worker1( haDb1, barrier1, barrier2, node1.getId(), node2.getId() ); w1.start();
        Worker2 w2 = new Worker2( haDb1, barrier1, barrier2, node1.getId(), node2.getId() ); w2.start();
        while ( w1.isAlive() || w2.isAlive() )
        {
            Thread.sleep( 500 );
        }
        boolean case1 = w2.successfull && !w2.deadlocked && !w1.successfull && w1.deadlocked;
        boolean case2 = !w2.successfull && w2.deadlocked && w1.successfull && !w1.deadlocked;
        assertTrue( case1 != case2 );
        assertTrue( case1 || case2  );
        
        ((HighlyAvailableGraphDatabase) haDb1).pullUpdates();
        ((HighlyAvailableGraphDatabase) haDb2).pullUpdates();
    }

    private static class Worker1 extends Thread
    {
        private final GraphDatabaseService db;
        private final long node1;
        private final long node2;
        private final CountDownLatch barrier1;
        private final CountDownLatch barrier2;
        
        private boolean successfull = false;
        private boolean deadlocked = false;
        
        Worker1( GraphDatabaseService db, CountDownLatch barrier1, CountDownLatch barrier2, long node1, long node2 )
        {
            this.db = db;
            this.barrier1 = barrier1;
            this.barrier2 = barrier2;
            this.node1 = node1;
            this.node2 = node2;
        }
        
        @Override
        public void run()
        {
            Transaction tx = db.beginTx();
            try
            {
                db.getNodeById( node1 ).setProperty( "1", "T1 1" );
                barrier2.countDown();
                try
                {
                    barrier1.await();
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                    e.printStackTrace();
                }
                db.getNodeById( node2 ).removeProperty( "2" );
                db.getNodeById( node1 ).removeProperty( "1" );
                System.out.println( "YAY worker1 won" );
                tx.success();
                successfull = true;
            }
            catch ( DeadlockDetectedException e )
            {
                System.out.println( "YAY worker1 deadlocked" );
                deadlocked = true;
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
            }
            finally
            {
                tx.finish();
            }
        }
    }

    private static class Worker2 extends Thread
    {
        private final GraphDatabaseService db;
        private final long node1;
        private final long node2;
        private final CountDownLatch barrier1;
        private final CountDownLatch barrier2;

        private boolean successfull = false;
        private boolean deadlocked = false;
        
        Worker2( GraphDatabaseService db, CountDownLatch barrier1, CountDownLatch barrier2, long node1, long node2 )
        {
            this.db = db;
            this.barrier1 = barrier1;
            this.barrier2 = barrier2;
            this.node1 = node1;
            this.node2 = node2;
        }
        
        @Override
        public void run()
        {
            Transaction tx = db.beginTx();
            try
            {
                db.getNodeById( node2 ).setProperty( "2", "T2 2" );
                barrier1.countDown();
                try
                {
                    barrier2.await();
                }
                catch ( InterruptedException e1 )
                {
                    Thread.interrupted();
                    e1.printStackTrace();
                }
                db.getNodeById( node1 ).setProperty( "1", "T2 2" );
                System.out.println( "YAY worker2 won" );
                tx.success();
                successfull = true;
            }
            catch ( DeadlockDetectedException e )
            {
                System.out.println( "YAY worker2 deadlock" );
                deadlocked = true;
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
            }
            finally
            {
                tx.finish();
            }
        }
    }
}
