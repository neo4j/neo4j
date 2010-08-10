package slavetest;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.shell.impl.RmiLocation;

public class MultiJvmTesting extends AbstractHaTest
{
    private static final int MASTER_PORT = 8990;
    
    private StandaloneDbCom masterJvm;
    private List<StandaloneDbCom> slaveJvms;
    
    protected void initializeDbs( int numSlaves ) throws Exception
    {
        try
        {
            initDeadMasterAndSlaveDbs( numSlaves );
            startUpMaster();
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

    @Override
    protected void pullUpdates( int... slaves ) throws Exception
    {
        if ( slaves.length == 0 )
        {
            for ( StandaloneDbCom db : slaveJvms )
            {
                db.pullUpdates();
            }
        }
        else
        {
            for ( StandaloneDbCom db : slaveJvms )
            {
                db.pullUpdates();
            }
        }
    }

    @Override
    protected <T> T executeJob( Job<T> job, int onSlave ) throws Exception
    {
        return slaveJvms.get( onSlave ).executeJob( job );
    }
    
    @Override
    protected <T> T executeJobOnMaster( Job<T> job ) throws Exception
    {
        return masterJvm.executeJob( job );
    }
    
    @Override
    protected void startUpMaster() throws Exception
    {
        masterJvm = spawnJvm( MASTER_PATH, MASTER_PORT, "-master", "true" );
    }
    
    @Override
    protected Job<Void> getMasterShutdownDispatcher()
    {
        return new CommonJobs.ShutdownJvm( masterJvm );
    }
    
    @Override
    protected Fetcher<DoubleLatch> getDoubleLatch() throws Exception
    {
        return new MultiJvmDLFetcher();
    }
}
