package slavetest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public abstract class AbstractHaTest
{
    static final RelationshipType REL_TYPE = DynamicRelationshipType.withName( "HA_TEST" );
    static final File PARENT_PATH = new File( "target/havar" );
    static final File DBS_PATH = new File( PARENT_PATH, "dbs" );
    static final File SKELETON_DB_PATH = new File( DBS_PATH, "skeleton" );
    static final Map<String, String> INDEX_CONFIG = MapUtil.stringMap( "index", "true" );
    
    private boolean expectsResults;
    private int nodeCount;
    private int relCount;
    private int nodePropCount;
    private int relPropCount;
    private int nodeIndexPropCount;
    
    protected static File dbPath( int num )
    {
        return new File( DBS_PATH, "" + num );
    }
    
    @Before
    public void clearExpectedResults()
    {
        expectsResults = false;
    }

    public void verify( VerifyDbContext refDb, VerifyDbContext... dbs )
    {
        for ( VerifyDbContext otherDb : dbs )
        {
            int vNodeCount = 0;
            int vRelCount = 0;
            int vNodePropCount = 0;
            int vRelPropCount = 0;
            int vNodeIndexPropCount = 0;
            
            Set<Node> otherNodes = IteratorUtil.addToCollection( otherDb.db.getAllNodes().iterator(),
                    new HashSet<Node>() );
            for ( Node node : refDb.db.getAllNodes() )
            {
                Node otherNode = otherDb.db.getNodeById( node.getId() );
                int[] counts = verifyNode( node, otherNode, refDb, otherDb );
                vRelCount += counts[0];
                vNodePropCount += counts[1];
                vRelPropCount += counts[2];
                vNodeIndexPropCount += counts[3];
                otherNodes.remove( otherNode );
                vNodeCount++;
            }
            assertTrue( otherNodes.isEmpty() );
            
            if ( expectsResults )
            {
                assertEquals( nodeCount, vNodeCount );
                assertEquals( relCount, vRelCount );
                assertEquals( nodePropCount, vNodePropCount );
                assertEquals( relPropCount, vRelPropCount );
                assertEquals( nodeIndexPropCount, vNodeIndexPropCount );
            }
        }
    }
    
    private static int[] verifyNode( Node node, Node otherNode,
            VerifyDbContext refDb, VerifyDbContext otherDb )
    {
        int vNodePropCount = verifyProperties( node, otherNode );
        int vNodeIndexPropCount = verifyIndex( node, otherNode, refDb, otherDb );
        Set<Long> otherRelIds = new HashSet<Long>();
        for ( Relationship otherRel : otherNode.getRelationships( Direction.OUTGOING ) )
        {
            otherRelIds.add( otherRel.getId() );
        }
        
        int vRelCount = 0;
        int vRelPropCount = 0;
        for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
        {
            Relationship otherRel = otherDb.db.getRelationshipById( rel.getId() );
            vRelPropCount += verifyProperties( rel, otherRel );
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
            vRelCount++;
        }
        
        if ( !otherRelIds.isEmpty() )
        {
            throw new RuntimeException( "Other node " + otherNode + " has more relationships " +
                    otherRelIds );
        }
        return new int[] { vRelCount, vNodePropCount, vRelPropCount, vNodeIndexPropCount };
    }

    private static int verifyIndex( Node node, Node otherNode, VerifyDbContext refDb,
            VerifyDbContext otherDb )
    {
        int count = 0;
        if ( refDb.index == null || otherDb.index == null )
        {
            return count;
        }
        
        Set<String> otherKeys = new HashSet<String>();
        for ( String key : otherNode.getPropertyKeys() )
        {
            if ( isIndexed( otherNode, otherDb, key ) )
            {
                otherKeys.add( key );
            }
        }
        count = otherKeys.size();
        
        for ( String key : node.getPropertyKeys() )
        {
            if ( otherKeys.remove( key ) != isIndexed( node, refDb, key ) )
            {
                throw new RuntimeException( "Index differs on " + node + ", " + key );
            }
        }
        if ( !otherKeys.isEmpty() )
        {
            throw new RuntimeException( "Other node " + otherNode + " has more indexing: " +
                    otherKeys );
        }
        return count;
    }

    private static boolean isIndexed( Node node, VerifyDbContext db, String key )
    {
        return db.index.getSingleNode( key, node.getProperty( key ) ) != null;
    }

    private static int verifyProperties( PropertyContainer entity, PropertyContainer otherEntity )
    {
        int count = 0;
        Set<String> otherKeys = IteratorUtil.addToCollection(
                otherEntity.getPropertyKeys().iterator(), new HashSet<String>() );
        for ( String key : entity.getPropertyKeys() )
        {
            Object value1 = entity.getProperty( key );
            Object value2 = otherEntity.getProperty( key );
            if ( !value1.equals( value2 ) )
            {
                throw new RuntimeException( entity + " not equals property '" + key + "': " +
                        value1 + ", " + value2 );
            }
            otherKeys.remove( key );
            count++;
        }
        if ( !otherKeys.isEmpty() )
        {
            throw new RuntimeException( "Other node " + otherEntity + " has more properties: " +
                    otherKeys );
        }
        return count;
    }

    public static <T> void assertCollection( Collection<T> collection, T... expectedItems )
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

    protected void createDeadDbs( int numSlaves ) throws IOException
    {
        FileUtils.deleteDirectory( PARENT_PATH );
        new EmbeddedGraphDatabase( SKELETON_DB_PATH.getAbsolutePath() ).shutdown();
        for ( int i = 0; i <= numSlaves; i++ )
        {
            FileUtils.copyDirectory( SKELETON_DB_PATH, dbPath( i ) );
        }
    }
    
    protected final void initializeDbs( int numSlaves ) throws Exception
    {
        initializeDbs( numSlaves, MapUtil.stringMap( ) );
    }

    protected abstract void initializeDbs( int numSlaves, Map<String, String> config ) throws Exception;
    
    protected abstract void pullUpdates( int... slaves ) throws Exception;
    
    protected abstract <T> T executeJob( Job<T> job, int onSlave ) throws Exception;

    protected abstract <T> T executeJobOnMaster( Job<T> job ) throws Exception;
    
    protected abstract void startUpMaster( Map<String, String> config ) throws Exception;

    protected abstract Job<Void> getMasterShutdownDispatcher();
    
    protected abstract void shutdownDbs() throws Exception;
    
    protected abstract Fetcher<DoubleLatch> getDoubleLatch() throws Exception;
    
    private class Worker extends Thread
    {
        private boolean successfull;
        private boolean deadlocked;
        private final int slave;
        private final Job<Boolean[]> job;
        
        Worker( int slave, Job<Boolean[]> job )
        {
            this.slave = slave;
            this.job = job;
        }
        
        @Override
        public void run()
        {
            try
            {
                Boolean[] result = executeJob( job, slave );
                successfull = result[0];
                deadlocked = result[1];
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
        }
    }
    
    protected void setExpectedResults( int nodeCount, int relCount,
            int nodePropCount, int relPropCount, int nodeIndexPropCount )
    {
        this.expectsResults = true;
        this.nodeCount = nodeCount;
        this.relCount = relCount;
        this.nodePropCount = nodePropCount;
        this.relPropCount = relPropCount;
        this.nodeIndexPropCount = nodeIndexPropCount;
    }
    
    @Test
    public void slaveCreateNode() throws Exception
    {
        setExpectedResults( 3, 2, 2, 2, 0 );
        initializeDbs( 1 );
        executeJob( new CommonJobs.CreateSomeEntitiesJob(), 0 );
    }
    
    @Test
    public void testMultipleSlaves() throws Exception
    {
        setExpectedResults( 2, 1, 1, 1, 0 );
        initializeDbs( 3 );
        executeJob( new CommonJobs.CreateSubRefNodeJob( CommonJobs.REL_TYPE.name(), null, null ), 0 );
        executeJob( new CommonJobs.SetSubRefPropertyJob( "name", "Hello" ), 1 );
        pullUpdates( 0, 2 );
    }

    // This is difficult to test a.t.m. since you can't really bring down a master
    // and expect it to be able to come up again without any work.
//    @Test
//    public void testMasterFailure() throws Exception
//    {
//        initializeDbs( 1 );
//        Serializable[] result = executeJob( new CommonJobs.CreateSubRefNodeMasterFailJob(
//                getMasterShutdownDispatcher() ), 0 );
//        assertFalse( (Boolean) result[0] );
//        startUpMaster( MapUtil.stringMap() );
//        long nodeId = (Long) result[1];
//        Boolean existed = executeJob( new CommonJobs.GetNodeByIdJob( nodeId ), 0 );
//        assertFalse( existed.booleanValue() );
//    }
    
    @Test
    public void testSlaveConstraintViolation() throws Exception
    {
        setExpectedResults( 2, 1, 0, 1, 0 );
        initializeDbs( 1 );
        
        Long nodeId = executeJob( new CommonJobs.CreateSubRefNodeJob(
                CommonJobs.REL_TYPE.name(), null, null ), 0 );
        Boolean successful = executeJob( new CommonJobs.DeleteNodeJob( nodeId.longValue(),
                false ), 0 );
        assertFalse( successful.booleanValue() );
    }
    
    @Test
    public void testMasterConstrainViolation() throws Exception
    {
        setExpectedResults( 2, 1, 1, 1, 0 );
        initializeDbs( 1 );
        
        Long nodeId = executeJob( new CommonJobs.CreateSubRefNodeJob( CommonJobs.REL_TYPE.name(),
                "name", "Mattias" ), 0 );
        Boolean successful = executeJobOnMaster(
                new CommonJobs.DeleteNodeJob( nodeId.longValue(), false ) );
        assertFalse( successful.booleanValue() );
        pullUpdates();
    }

    @Test
    public void testGetRelationships() throws Exception
    {
        setExpectedResults( 3, 2, 0, 0, 0 );
        initializeDbs( 1 );
        
        assertEquals( (Integer) 1, executeJob( new CommonJobs.CreateSubRefNodeWithRelCountJob(
                CommonJobs.REL_TYPE.name(), CommonJobs.REL_TYPE.name(), CommonJobs.KNOWS.name() ), 0 ) );
        assertEquals( (Integer) 2, executeJob( new CommonJobs.CreateSubRefNodeWithRelCountJob(
                CommonJobs.REL_TYPE.name(), CommonJobs.REL_TYPE.name(), CommonJobs.KNOWS.name() ), 0 ) );
        assertEquals( (Integer) 2, executeJob( new CommonJobs.GetRelationshipCountJob(
                CommonJobs.REL_TYPE.name(), CommonJobs.KNOWS.name() ), 0 ) );
        assertEquals( (Integer) 2, executeJobOnMaster( new CommonJobs.GetRelationshipCountJob(
                CommonJobs.REL_TYPE.name(), CommonJobs.KNOWS.name() ) ) );
    }

    @Test
    public void testNoTransaction() throws Exception
    {
        setExpectedResults( 2, 1, 0, 1, 0 );
        initializeDbs( 1 );
        
        executeJobOnMaster( new CommonJobs.CreateSubRefNodeJob(
                CommonJobs.REL_TYPE.name(), null, null ) );
        assertFalse( executeJob( new CommonJobs.CreateNodeOutsideOfTxJob(), 0 ).booleanValue() );
        assertFalse( executeJobOnMaster( new CommonJobs.CreateNodeOutsideOfTxJob() ).booleanValue() );
    }

    @Test
    public void testNodeDeleted() throws Exception
    {
        setExpectedResults( 1, 0, 0, 0, 0 );
        initializeDbs( 1 );
        
        Long nodeId = executeJobOnMaster( new CommonJobs.CreateNodeJob() );
        pullUpdates();
        assertTrue( executeJobOnMaster( new CommonJobs.DeleteNodeJob(
                nodeId.longValue(), false ) ).booleanValue() );
        assertFalse( executeJob( new CommonJobs.SetNodePropertyJob( nodeId.longValue(), "something",
                "some thing" ), 0 ) );
    }

    @Test
    public void testDeadlock() throws Exception
    {
        initializeDbs( 2 );
        
        Long[] nodes = executeJobOnMaster( new CommonJobs.CreateNodesJob( 2 ) );
        pullUpdates();
        
        Fetcher<DoubleLatch> fetcher = getDoubleLatch();
        Worker w1 = new Worker( 0, new CommonJobs.Worker1Job( nodes[0], nodes[1], fetcher ) );
        Worker w2 = new Worker( 1, new CommonJobs.Worker2Job( nodes[0], nodes[1], fetcher ) );
        w1.start();
        w2.start();
        while ( w1.isAlive() || w2.isAlive() )
        {
            Thread.sleep( 500 );
        }
        boolean case1 = w2.successfull && !w2.deadlocked && !w1.successfull && w1.deadlocked;
        boolean case2 = !w2.successfull && w2.deadlocked && w1.successfull && !w1.deadlocked;
        assertTrue( case1 != case2 );
        assertTrue( case1 || case2  );
        pullUpdates();
    }
    
    @Test
    public void createNodeAndIndex() throws Exception
    {
        setExpectedResults( 2, 0, 1, 0, 1 );
        initializeDbs( 1, INDEX_CONFIG );
        executeJob( new CommonJobs.CreateNodeAndIndexJob( "name", "Neo" ), 0 );
    }
    
    @Test
    public void indexingAndTwoSlaves() throws Exception
    {
        initializeDbs( 2, INDEX_CONFIG );
        long id = executeJobOnMaster( new CommonJobs.CreateNodeAndIndexJob( "name", "Morpheus" ) );
//        pullUpdates();
        long id2 = executeJobOnMaster( new CommonJobs.CreateNodeAndIndexJob( "name", "Trinity" ) );
//        executeJob( new CommonJobs.AddIndex( id, MapUtil.map( "key1",
//                new String[] { "value1", "value2" }, "key 2", 105.43f ) ), 1 );
        pullUpdates();
    }
}
