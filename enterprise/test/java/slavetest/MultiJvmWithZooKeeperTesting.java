package slavetest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

public class MultiJvmWithZooKeeperTesting extends MultiJvmTesting
{
    private static final File BASE_ZOO_KEEPER_DATA_DIR =
            new File( new File( "target" ), "zookeeper-data" );
    private static final int BASE_ZOO_KEEPER_CLIENT_PORT = 2180;
    private static final int BASE_ZOO_KEEPER_SERVER_START_PORT = 2888;
    private static final int BASE_ZOO_KEEPER_SERVER_END_PORT = 3888;
    private static final int BASE_HA_SERVER_PORT = 5559;
    private static final int ZOO_KEEPER_CLUSTER_SIZE = 3;
    
    private static Collection<Runnable> zooKeeperClusterShutdownHooks;
    
    @BeforeClass
    public static void startZooKeeperCluster() throws Exception
    {
        FileUtils.deleteDirectory( BASE_ZOO_KEEPER_DATA_DIR );
        zooKeeperClusterShutdownHooks = new ArrayList<Runnable>();
        BASE_ZOO_KEEPER_DATA_DIR.mkdirs();
        for ( int i = 0; i < ZOO_KEEPER_CLUSTER_SIZE; i++ )
        {
            File configFile = writeZooKeeperConfigFile( ZOO_KEEPER_CLUSTER_SIZE, i+1 );
            final Process process = Runtime.getRuntime().exec( new String[] { "java", "-cp",
                    System.getProperty( "java.class.path" ),
                    "org.apache.zookeeper.server.quorum.QuorumPeerMain",
                    configFile.getAbsolutePath() } );
            zooKeeperClusterShutdownHooks.add( new Runnable()
            {
                public void run()
                {
                    process.destroy();
                }
            } );
        }
        Thread.sleep( 5000 );
    }
    
    @Override
    protected StandaloneDbCom spawnJvm( int numServers, File path, int port, int machineId,
            String... extraArgs ) throws Exception
    {
        List<String> myExtraArgs = new ArrayList<String>();
        myExtraArgs.add( "-" + HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID );
        myExtraArgs.add( "" + (machineId+1) );
        myExtraArgs.add( "-" + HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS );
        myExtraArgs.add( buildZooKeeperServersConfigValue( ZOO_KEEPER_CLUSTER_SIZE ) );
        myExtraArgs.add( "-" + HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVERS );
        myExtraArgs.add( buildHaServersConfigValue( numServers ) );
        myExtraArgs.addAll( Arrays.asList( extraArgs ) );
        return super.spawnJvm( numServers, path, port, machineId, myExtraArgs.toArray(
                new String[myExtraArgs.size()] ) );
    }
    
    private String buildHaServersConfigValue( int numServers )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < numServers; i++ )
        {
            builder.append( (i > 0 ? "," : "") + (i+1) + "=localhost:" +
                    (BASE_HA_SERVER_PORT + i) );
        }
        return builder.toString();
    }

    private static String buildZooKeeperServersConfigValue( int zooKeeperClusterSize )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < zooKeeperClusterSize; i++ )
        {
            builder.append( (i > 0 ? "," : "") + "localhost:" + clientPort( i+1 ) );
        }
        return builder.toString();
    }

    private static File writeZooKeeperConfigFile( int size, int id ) throws IOException
    {
        File configFile = new File( BASE_ZOO_KEEPER_DATA_DIR, "config" + id + ".cfg" );
        Properties props = new Properties();
        populateZooConfig( props, size, id );
        FileWriter writer = null;
        writer = new FileWriter( configFile );
        for ( Object key : new TreeSet<Object>( props.keySet() ) )
        {
            writer.write( key + " = " + props.get( key ) + "\n" );
        }
        writer.close();
        
        writer = new FileWriter( new File( dataDir( id ), "myid" ) );
        writer.write( "" + id );
        writer.close();
        return configFile;
    }
    
    private static File dataDir( int id )
    {
        File dir = new File( BASE_ZOO_KEEPER_DATA_DIR, "" + id );
        dir.mkdirs();
        return dir;
    }
    
    private static int clientPort( int id )
    {
        return BASE_ZOO_KEEPER_CLIENT_PORT + id;
    }
    
    private static void populateZooConfig( Properties props, int size, int id )
    {
        props.setProperty( "tickTime", "2000" );
        props.setProperty( "initLimit", "10" );
        props.setProperty( "syncLimit", "5" );
        props.setProperty( "clientPort", "" + clientPort( id ) );
        props.setProperty( "dataDir", dataDir( id ).getPath() );
        for ( int i = 1; i <= size; i++ )
        {
            props.setProperty( "server." + i, "localhost:" + serverStartPort( i ) +
                    ":" + serverEndPort( i ) );
        }
    }

    private static int serverEndPort( int id )
    {
        return BASE_ZOO_KEEPER_SERVER_END_PORT + ((id-1));
    }

    private static int serverStartPort( int id )
    {
        return BASE_ZOO_KEEPER_SERVER_START_PORT + ((id-1));
    }
    
    @AfterClass
    public static void shutdownZooKeeperCluster()
    {
        for ( Runnable hook : zooKeeperClusterShutdownHooks )
        {
            hook.run();
        }
    }
    
    @Override
    public void slaveCreateNode() throws Exception
    {
        // TODO Auto-generated method stub
        super.slaveCreateNode();
    }
}
