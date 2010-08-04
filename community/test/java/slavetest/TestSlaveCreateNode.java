package slavetest;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

public class TestSlaveCreateNode
{
    private static final RelationshipType REL_TYPE = DynamicRelationshipType.withName( "HA_TEST" );
    private static final File MASTER_PATH = new File( "target/dbs/master" );
    private static final File SLAVE_PATH = new File( "target/dbs/slave" );
    private static FakeMaster master;
    private static FakeBroker broker;
    private static GraphDatabaseService haDb;
    
    public void initializeDbs()
    {
        try
        {
            FileUtils.deleteDirectory( SLAVE_PATH );
            FileUtils.deleteDirectory( MASTER_PATH );
            GraphDatabaseService masterDb =
                    new EmbeddedGraphDatabase( MASTER_PATH.getAbsolutePath() );
            masterDb.shutdown();
            FileUtils.copyDirectory( MASTER_PATH, SLAVE_PATH );
            master = new FakeMaster( MASTER_PATH.getAbsolutePath() );
            broker = new FakeBroker( master ); 
            haDb = new HighlyAvailableGraphDatabase( SLAVE_PATH.getAbsolutePath(),
                    new HashMap<String, String>(), broker );
            broker.setSlave( haDb );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    @Test
    public void slaveCreateNode()
    {
        initializeDbs();
        Transaction tx = haDb.beginTx();
        try
        {
            Node node = haDb.createNode();
            haDb.getReferenceNode().createRelationshipTo( node, REL_TYPE );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        verify( master.getGraphDb(), haDb );
    }

    private static void verify( GraphDatabaseService refDb, GraphDatabaseService... dbs )
    {
        for ( Node node : refDb.getAllNodes() )
        {
            for ( GraphDatabaseService otherDb : dbs )
            {
                Node otherNode = otherDb.getNodeById( node.getId() );
                verifyNode( node, otherNode, otherDb );
            }
        }
    }

    private static void verifyNode( Node node, Node otherNode, GraphDatabaseService otherDb )
    {
        verifyProperties( node, otherNode );
        Set<Long> otherRelIds = new HashSet<Long>();
        for ( Relationship otherRel : otherNode.getRelationships( Direction.OUTGOING ) )
        {
            otherRelIds.add( otherRel.getId() );
        }
        
        for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
        {
            Relationship otherRel = otherDb.getRelationshipById( rel.getId() );
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
