/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;

public abstract class AbstractHaTest
{
    static final RelationshipType REL_TYPE = DynamicRelationshipType.withName( "HA_TEST" );
    static final File PARENT_PATH = new File( "target/havar" );
    static final File DBS_PATH = new File( PARENT_PATH, "dbs" );
    static final File SKELETON_DB_PATH = new File( DBS_PATH, "skeleton" );

    private boolean expectsResults;
    private int nodeCount;
    private int relCount;
    private int nodePropCount;
    private int relPropCount;
    private int nodeIndexServicePropCount;
    private int nodeIndexProviderPropCount;

    public @Rule
    TestName testName = new TestName()
    {
        @Override
        public String getMethodName()
        {
            return AbstractHaTest.this.getClass().getName() + "." + super.getMethodName();
        }
    };

    protected static File dbPath( int num )
    {
        return new File( DBS_PATH, "" + num );
    }

    @Before
    public void clearExpectedResults() throws Exception
    {
        clearDbs();
        expectsResults = false;
    }

    public void verify( GraphDatabaseService refDb, GraphDatabaseService... dbs )
    {
        for ( GraphDatabaseService otherDb : dbs )
        {
            int vNodeCount = 0;
            int vRelCount = 0;
            int vNodePropCount = 0;
            int vRelPropCount = 0;
            int vNodeIndexServicePropCount = 0;
            int vNodeIndexProviderPropCount = 0;

            Set<Node> otherNodes = IteratorUtil.addToCollection( otherDb.getAllNodes().iterator(),
                    new HashSet<Node>() );
            for ( Node node : refDb.getAllNodes() )
            {
                Node otherNode = otherDb.getNodeById( node.getId() );
                int[] counts = verifyNode( node, otherNode, refDb, otherDb );
                vRelCount += counts[0];
                vNodePropCount += counts[1];
                vRelPropCount += counts[2];
                vNodeIndexServicePropCount += counts[3];
                vNodeIndexProviderPropCount += counts[4];
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
                // assertEquals( nodeIndexServicePropCount, vNodeIndexServicePropCount );
                assertEquals( nodeIndexProviderPropCount, vNodeIndexProviderPropCount );
            }
        }
    }

    private static int[] verifyNode( Node node, Node otherNode,
            GraphDatabaseService refDb, GraphDatabaseService otherDb )
    {
        int vNodePropCount = verifyProperties( node, otherNode );
//        int vNodeIndexServicePropCount = verifyIndexService( node, otherNode, refDb, otherDb );
        int vNodeIndexProviderProCount = verifyIndexProvider( node, otherNode, refDb, otherDb );
        Set<Long> otherRelIds = new HashSet<Long>();
        for ( Relationship otherRel : otherNode.getRelationships( Direction.OUTGOING ) )
        {
            otherRelIds.add( otherRel.getId() );
        }

        int vRelCount = 0;
        int vRelPropCount = 0;
        for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
        {
            Relationship otherRel = otherDb.getRelationshipById( rel.getId() );
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
            fail( "Other node " + otherNode + " has more relationships " + otherRelIds );
        }
        return new int[] { vRelCount, vNodePropCount, vRelPropCount, -1, vNodeIndexProviderProCount };
    }

/*    private static int verifyIndexService( Node node, Node otherNode, VerifyDbContext refDb,
            VerifyDbContext otherDb )
    {
        return 0;
        int count = 0;
        if ( refDb.indexService == null || otherDb.indexService == null )
        {
            return count;
        }

        Set<String> otherKeys = new HashSet<String>();
        for ( String key : otherNode.getPropertyKeys() )
        {
            if ( isIndexedWithIndexService( otherNode, otherDb, key ) )
            {
                otherKeys.add( key );
            }
        }
        count = otherKeys.size();

        for ( String key : node.getPropertyKeys() )
        {
            if ( otherKeys.remove( key ) != isIndexedWithIndexService( node, refDb, key ) )
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
    }*/

//    private static boolean isIndexedWithIndexService( Node node, VerifyDbContext db, String key )
//    {
//        return false;
//        // return db.indexService.getSingleNode( key, node.getProperty( key ) ) != null;
//    }

    /**
     * This method is bogus... it really needs to ask all indexes, not the "users" index :)
     */
    private static int verifyIndexProvider( Node node, Node otherNode, GraphDatabaseService refDb,
            GraphDatabaseService otherDb )
    {
        int count = 0;
        Set<String> otherKeys = new HashSet<String>();
        for ( String key : otherNode.getPropertyKeys() )
        {
            if ( isIndexedWithIndexProvider( otherNode, otherDb, key ) )
            {
                otherKeys.add( key );
            }
        }
        count = otherKeys.size();

        for ( String key : node.getPropertyKeys() )
        {
            if ( otherKeys.remove( key ) != isIndexedWithIndexProvider( node, refDb, key ) )
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

    private static boolean isIndexedWithIndexProvider( Node node, GraphDatabaseService db, String key )
    {
        return db.index().forNodes( "users" ).get( key, node.getProperty( key ) ).getSingle() != null;
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
            if ( value1.getClass().isArray() && value2.getClass().isArray() )
            {
            }
            else if ( !value1.equals( value2 ) )
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

    private void clearDbs() throws IOException
    {
        FileUtils.deleteDirectory( PARENT_PATH );
    }

    protected void initializeDbs( int numSlaves ) throws Exception
    {
        initializeDbs( numSlaves, MapUtil.stringMap() );
    }

    protected void initializeDbs( int numSlaves, Map<String, String> config ) throws Exception
    {
        startUpMaster( config );
        for ( int i = 0; i < numSlaves; i++ )
        {
            addDb( config );
        }
    }

    protected abstract void awaitAllStarted() throws Exception;

    protected abstract void addDb( Map<String, String> config ) throws Exception;

    protected abstract void pullUpdates( int... slaves ) throws Exception;

    protected abstract <T> T executeJob( Job<T> job, int onSlave ) throws Exception;

    protected abstract <T> T executeJobOnMaster( Job<T> job ) throws Exception;

    protected abstract void startUpMaster( Map<String, String> config ) throws Exception;

    protected abstract CommonJobs.ShutdownDispatcher getMasterShutdownDispatcher();

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
            int nodePropCount, int relPropCount, int nodeIndexServicePropCount, int nodeIndexProviderPropCount )
    {
        this.expectsResults = true;
        this.nodeCount = nodeCount;
        this.relCount = relCount;
        this.nodePropCount = nodePropCount;
        this.relPropCount = relPropCount;
        this.nodeIndexServicePropCount = nodeIndexServicePropCount;
        this.nodeIndexProviderPropCount = nodeIndexProviderPropCount;
    }

    @Test
    public void slaveCreateNode() throws Exception
    {
        setExpectedResults( 3, 2, 2, 2, 0, 0 );
        initializeDbs( 1 );
        executeJob( new CommonJobs.CreateSomeEntitiesJob(), 0 );
    }

    @Test
    public void testMultipleSlaves() throws Exception
    {
        setExpectedResults( 2, 1, 1, 1, 0, 0 );
        initializeDbs( 3 );
        executeJob( new CommonJobs.CreateSubRefNodeJob( CommonJobs.REL_TYPE.name(), null, null ), 0 );
        executeJob( new CommonJobs.SetSubRefPropertyJob( "name", "Hello" ), 1 );
        pullUpdates( 0, 2 );
    }

    // This is difficult to test a.t.m. since you can't really bring down a master
    // and expect it to be able to come up again without any work.
    @Test
    public void testMasterFailure() throws Exception
    {
        initializeDbs( 1 );
        Serializable[] result = executeJob( new CommonJobs.CreateSubRefNodeMasterFailJob(
                getMasterShutdownDispatcher() ), 0 );
        assertFalse( (Boolean) result[0] );
        startUpMaster( MapUtil.stringMap() );
        long nodeId = (Long) result[1];
        Boolean existed = executeJob( new CommonJobs.GetNodeByIdJob( nodeId ), 0 );
        assertFalse( existed.booleanValue() );
    }

    @Test
    public void testSlaveConstraintViolation() throws Exception
    {
        setExpectedResults( 2, 1, 0, 1, 0, 0 );
        initializeDbs( 1 );

        Long nodeId = executeJob( new CommonJobs.CreateSubRefNodeJob(
                CommonJobs.REL_TYPE.name(), null, null ), 0 );
        Boolean successful = executeJob( new CommonJobs.DeleteNodeJob( nodeId.longValue(),
                false ), 0 );
        assertFalse( successful.booleanValue() );
    }

    @Test
    public void testMasterConstraintViolation() throws Exception
    {
        setExpectedResults( 2, 1, 1, 1, 0, 0 );
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
        setExpectedResults( 3, 2, 0, 0, 0, 0 );
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
        setExpectedResults( 2, 1, 0, 1, 0, 0 );
        initializeDbs( 1 );

        executeJobOnMaster( new CommonJobs.CreateSubRefNodeJob(
                CommonJobs.REL_TYPE.name(), null, null ) );
        assertFalse( executeJob( new CommonJobs.CreateNodeOutsideOfTxJob(), 0 ).booleanValue() );
        assertFalse( executeJobOnMaster( new CommonJobs.CreateNodeOutsideOfTxJob() ).booleanValue() );
    }

    @Test
    public void testNodeDeleted() throws Exception
    {
        setExpectedResults( 1, 0, 0, 0, 0, 0 );
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
        w1.join();
        w2.join();
        boolean case1 = w2.successfull && !w2.deadlocked && !w1.successfull && w1.deadlocked;
        boolean case2 = !w2.successfull && w2.deadlocked && w1.successfull && !w1.deadlocked;
        assertTrue( case1 != case2 );
        assertTrue( case1 || case2  );
        pullUpdates();
    }

    @Test
    public void createNodeAndIndex() throws Exception
    {
        setExpectedResults( 2, 0, 1, 0, 1, 0 );
        initializeDbs( 1 );
        executeJob( new CommonJobs.CreateNodeAndIndexJob( "name", "Neo" ), 0 );
    }

    @Test
    public void indexingAndTwoSlaves() throws Exception
    {
        initializeDbs( 2 );
        long id = executeJobOnMaster( new CommonJobs.CreateNodeAndIndexJob( "name", "Morpheus" ) );
        pullUpdates();
        long id2 = executeJobOnMaster( new CommonJobs.CreateNodeAndIndexJob( "name", "Trinity" ) );
        executeJob( new CommonJobs.AddIndex( id, MapUtil.map( "key1",
                new String[] { "value1", "value2" }, "key 2", 105.43f ) ), 1 );
        pullUpdates();
    }

    @Test
    public void testNewIndexFramework() throws Exception
    {
        setExpectedResults( 2, 0, 2, 0, 0, 2 );
        initializeDbs( 2 );
        long id = executeJobOnMaster( new CommonJobs.CreateNodeAndNewIndexJob( "users",
                "name", "Morpheus", "rank", "Captain" ) );
        pullUpdates();
    }

    @Test
    public void testLargeTransaction() throws Exception
    {
        initializeDbs( 1 );
        executeJob( new CommonJobs.LargeTransactionJob( 20, 1 ), 0 );
    }

    @Test
    public void testPullLargeTransaction() throws Exception
    {
        initializeDbs( 1 );
        executeJobOnMaster( new CommonJobs.LargeTransactionJob( 20, 1 ) );
        pullUpdates();
    }

    @Test
    public void testLargeTransactionData() throws Exception
    {
        initializeDbs( 1 );
        executeJob( new CommonJobs.LargeTransactionJob( 1, 20 ), 0 );
    }

    @Test
    public void testPullLargeTransactionData() throws Exception
    {
        initializeDbs( 1 );
        executeJobOnMaster( new CommonJobs.LargeTransactionJob( 1, 20 ) );
        pullUpdates();
    }

    @Test
    public void makeSureSlaveCanCopyLargeInitialDatabase() throws Exception
    {
        startUpMaster( MapUtil.stringMap() );
        executeJobOnMaster( new CommonJobs.LargeTransactionJob( 1, 60 ) );
        addDb( MapUtil.stringMap() );
        awaitAllStarted();
        executeJob( new CommonJobs.CreateSubRefNodeJob( "whatever", "my_key", "my_value" ), 0 );
    }

    @Test
    public void canCopyInitialDbWithLuceneIndexes() throws Exception
    {
        int additionalNodeCount = 50;
        setExpectedResults( 1+additionalNodeCount, 0, additionalNodeCount*2, 0, 0, additionalNodeCount*2 );
        startUpMaster( MapUtil.stringMap() );
        for ( int i = 0; i < additionalNodeCount; i++ )
        {
            executeJobOnMaster( new CommonJobs.CreateNodeAndNewIndexJob( "users",
                    "the key " + i, "the best value",
                    "a key " + i, "the worst value" ) );
        }
        addDb( MapUtil.stringMap() );
        awaitAllStarted();
    }
}
