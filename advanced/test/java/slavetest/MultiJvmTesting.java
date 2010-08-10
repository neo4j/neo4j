package slavetest;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.shell.impl.RmiLocation;

import static org.junit.Assert.assertEquals;
import static slavetest.BasicHaTesting.verify;

public class MultiJvmTesting
{
    private static final RelationshipType REL_TYPE = DynamicRelationshipType.withName( "HA_TEST" );
    private static final File PARENT_PATH = new File( "target/dbs" );
    private static final File MASTER_PATH = new File( PARENT_PATH, "master" );
    private static final File SLAVE_PATH = new File( PARENT_PATH, "slave" );
    private static final int MASTER_PORT = 8990;
    
    private StandaloneDbCom masterJvm;
    private List<StandaloneDbCom> slaveJvms;
    
    private void initializeDbs( int numSlaves ) throws Exception
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
            
            masterJvm = spawnJvm( MASTER_PATH, MASTER_PORT, "-master", "true" );
            slaveJvms = new ArrayList<StandaloneDbCom>();
            for ( int i = 0; i < numSlaves; i++ )
            {
                File slavePath = slavePath( i );
                StandaloneDbCom slaveJvm = spawnJvm( slavePath, MASTER_PORT + 1 + i, "-id",
                        "" + i );
                slaveJvms.add( slaveJvm );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    @After
    public void shutdownDbsAndVerify() throws Exception
    {
        for ( StandaloneDbCom slave : slaveJvms )
        {
            slave.shutdown();
        }
        masterJvm.shutdown();
        
        GraphDatabaseService masterDb = new EmbeddedGraphDatabase( MASTER_PATH.getAbsolutePath() );
        for ( int i = 0; i < slaveJvms.size(); i++ )
        {
            GraphDatabaseService slaveDb =
                    new EmbeddedGraphDatabase( slavePath( i ).getAbsolutePath() );
            verify( masterDb, slaveDb );
        }
    }

    private StandaloneDbCom spawnJvm( File path, int port, String... extraArgs ) throws Exception
    {
        Collection<String> list = new ArrayList<String>( Arrays.asList(
                "java", "-cp", System.getProperty( "java.class.path" ),
                StandaloneDb.class.getName(),
                "-path", path.getAbsolutePath(),
                "-port", "" + port ) );
        list.addAll( Arrays.asList( extraArgs ) );
        
        Runtime.getRuntime().exec( list.toArray( new String[list.size()] ) );
        StandaloneDbCom result = null;
        long startTime = System.currentTimeMillis();
        RmiLocation location = RmiLocation.location( "localhost", port, "interface" );
        RemoteException latestException = null;
        while ( result == null && (System.currentTimeMillis() - startTime) < 1000*10 )
        {
            try
            {
                result = (StandaloneDbCom) location.getBoundObject();
            }
            catch ( RemoteException e )
            {
                latestException = e;
                // OK, just retry
                try
                {
                    Thread.sleep( 200 );
                }
                catch ( InterruptedException ee )
                { // OK
                }
            }
        }
        if ( result == null )
        {
            throw latestException;
        }
        return result;
    }

    private static File slavePath( int num )
    {
        return new File( SLAVE_PATH, "" + num );
    }
    
    @Ignore
    @Test
    public void testTwoSlaves() throws Exception
    {
        initializeDbs( 2 );
        StandaloneDbCom db1 = slaveJvms.get( 0 );
        StandaloneDbCom db2 = slaveJvms.get( 1 );
        final String name = "Mattias";
        db1.executeJob( new Job<Void>()
        {
            public Void execute( GraphDatabaseService db )
            {
                Transaction tx = db.beginTx();
                try
                {
                    Node node = db.createNode();
                    db.getReferenceNode().createRelationshipTo( node, REL_TYPE );
                    node.setProperty( "name", name );
                    tx.success();
                }
                finally
                {
                    tx.finish();
                }
                return null;
            }
        } );
        
        String readName = db2.executeJob( new Job<String>()
        {
            public String execute( GraphDatabaseService db )
            {
                Transaction tx = db.beginTx();
                try
                {
                    Node refNode = db.getReferenceNode();
                    // To force it to pull updates
                    refNode.removeProperty( "yoyoyoyo" );
                    Node node = refNode.getSingleRelationship( REL_TYPE,
                            Direction.OUTGOING ).getEndNode();
                    String name = (String) node.getProperty( "name" );
                    node.setProperty( "title", "Whatever" );
                    tx.success();
                    return name;
                }
                finally
                {
                    tx.finish();
                }
            }
        } );
        
        assertEquals( name, readName );
        db1.pullUpdates();
    }
}
