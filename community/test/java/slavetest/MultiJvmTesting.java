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
    
    private List<StandaloneDbCom> jvms;
    
    protected void initializeDbs( int numSlaves ) throws Exception
    {
        jvms = new ArrayList<StandaloneDbCom>();
        try
        {
            initDeadDbs( numSlaves );
            startUpMaster( numSlaves );
            for ( int i = 1; i <= numSlaves; i++ )
            {
                File slavePath = dbPath( i );
                StandaloneDbCom slaveJvm = spawnJvm( numSlaves, slavePath, MASTER_PORT + i, i );
                jvms.add( slaveJvm );
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
        System.out.println( "shut down" );
        shutdownDbs();
        
        System.out.println( "verify" );
        GraphDatabaseService masterDb = new EmbeddedGraphDatabase( dbPath( 0 ).getAbsolutePath() );
        for ( int i = 1; i < jvms.size(); i++ )
        {
            GraphDatabaseService slaveDb =
                    new EmbeddedGraphDatabase( dbPath( i ).getAbsolutePath() );
            verify( masterDb, slaveDb );
        }
        System.out.println( "verified" );
    }

    protected void shutdownDbs() throws Exception
    {
        for ( StandaloneDbCom slave : jvms )
        {
            slave.initiateShutdown();
        }
        for ( int i = 0; i < jvms.size(); i++ )
        {
            waitUntilShutdownFileFound( dbPath( i ) );
        }
    }

    protected void waitUntilShutdownFileFound( File slavePath ) throws Exception
    {
        File file = new File( slavePath, "shutdown" );
        while ( !file.exists() )
        {
            Thread.sleep( 100 );
        }
    }

    protected StandaloneDbCom spawnJvm( int numServers, File path, int port, int machineId,
            String... extraArgs ) throws Exception
    {
        Collection<String> list = new ArrayList<String>( Arrays.asList(
                "java", "-cp", System.getProperty( "java.class.path" ),
                StandaloneDb.class.getName(),
                "-path", path.getAbsolutePath(),
                "-port", "" + port,
                "-id", "" + machineId ) );
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
            for ( int i = 1; i < jvms.size(); i++ )
            {
                jvms.get( i ).pullUpdates();
            }
        }
        else
        {
            for ( int slave : slaves )
            {
                jvms.get( slave+1 ).pullUpdates();
            }
        }
    }

    @Override
    protected <T> T executeJob( Job<T> job, int onSlave ) throws Exception
    {
        return jvms.get( onSlave+1 ).executeJob( job );
    }
    
    @Override
    protected <T> T executeJobOnMaster( Job<T> job ) throws Exception
    {
        return jvms.get( 0 ).executeJob( job );
    }
    
    @Override
    protected void startUpMaster( int numSlaves ) throws Exception
    {
        jvms.add( spawnJvm( numSlaves, dbPath( 0 ), MASTER_PORT, 0, "-master", "true" ) );
    }
    
    @Override
    protected Job<Void> getMasterShutdownDispatcher()
    {
        return new CommonJobs.ShutdownJvm( jvms.get( 0 ) );
    }
    
    @Override
    protected Fetcher<DoubleLatch> getDoubleLatch() throws Exception
    {
        return new MultiJvmDLFetcher();
    }
}
