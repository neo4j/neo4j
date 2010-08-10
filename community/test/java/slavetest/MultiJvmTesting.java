package slavetest;

import static org.junit.Assert.assertEquals;
import static slavetest.BasicHaTesting.verify;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.shell.impl.RmiLocation;

public class MultiJvmTesting
{
    private static final RelationshipType REL_TYPE = DynamicRelationshipType.withName( "HA_TEST" );
    private static final File PARENT_PATH = new File( "target/mdbs" );
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
            slave.initiateShutdown();
        }
        masterJvm.initiateShutdown();
        for ( int i = 0; i < slaveJvms.size(); i++ )
        {
            waitUntilShutdownFileFound( slavePath( i ) );
        }
        waitUntilShutdownFileFound( MASTER_PATH );
        
        GraphDatabaseService masterDb = new EmbeddedGraphDatabase( MASTER_PATH.getAbsolutePath() );
        for ( int i = 0; i < slaveJvms.size(); i++ )
        {
            GraphDatabaseService slaveDb =
                    new EmbeddedGraphDatabase( slavePath( i ).getAbsolutePath() );
            verify( masterDb, slaveDb );
        }
    }

    private void waitUntilShutdownFileFound( File slavePath ) throws Exception
    {
        File file = new File( slavePath, "shutdown" );
        while ( !file.exists() )
        {
            Thread.sleep( 100 );
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
    
    @Test
    public void testTwoSlaves() throws Exception
    {
        initializeDbs( 2 );
        StandaloneDbCom db1 = slaveJvms.get( 0 );
        StandaloneDbCom db2 = slaveJvms.get( 1 );
        final String name = "Mattias";
        db1.executeJob( new CommonJobs.CreateSubRefNodeJob( name ) );
        String readName = db2.executeJob( new CommonJobs.SetSubRefNameJob() );
        
        assertEquals( name, readName );
        db1.pullUpdates();
    }
}
