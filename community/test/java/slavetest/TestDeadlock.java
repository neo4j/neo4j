package slavetest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

public class TestDeadlock
{
    private static final RelationshipType REL_TYPE = DynamicRelationshipType.withName( "HA_TEST" );
    private static final File PARENT_PATH = new File( "target/dbs" );
    private static final File MASTER_PATH = new File( PARENT_PATH, "master" );
    private static final File SLAVE_PATH = new File( PARENT_PATH, "slaves" );
    
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
    
    @After
    public void verifyAndShutdownDbs()
    {
//        System.out.println( "ONLINE VERIFICATION" );
//        verify( master.getGraphDb(), haDbs.toArray( new GraphDatabaseService[haDbs.size()] ) );
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
}
