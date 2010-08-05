package slavetest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
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
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

public class TestSlaveCreateNode
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
}
