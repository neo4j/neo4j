package slavetest;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.Master;

public class TestSlaveCreateNode
{
    private static final RelationshipType REL_TYPE = DynamicRelationshipType.withName( "HA_TEST" );
    private static final File MASTER_PATH = new File( "target/dbs/master" );
    private static final File SLAVE_PATH = new File( "target/dbs/slave" );
    
    @BeforeClass
    public static void initializeDbs()
    {
        try
        {
            FileUtils.deleteDirectory( SLAVE_PATH );
            FileUtils.deleteDirectory( MASTER_PATH );
            GraphDatabaseService masterDb =
                    new EmbeddedGraphDatabase( MASTER_PATH.getAbsolutePath() );
            masterDb.shutdown();
            FileUtils.copyDirectory( MASTER_PATH, SLAVE_PATH );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    @Test
    public void slaveCreateNode()
    {
        Master master = new FakeMaster( MASTER_PATH.getAbsolutePath() );
        FakeBroker broker = new FakeBroker( master ); 
        GraphDatabaseService haDb = new HighlyAvailableGraphDatabase( SLAVE_PATH.getAbsolutePath(),
                new HashMap<String, String>(), broker );
        broker.setSlave( haDb );
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
    }
}
