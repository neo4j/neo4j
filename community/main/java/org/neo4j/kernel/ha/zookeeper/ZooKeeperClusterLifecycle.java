package org.neo4j.kernel.ha.zookeeper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.TreeSet;

public class ZooKeeperClusterLifecycle
{
    private final int size;
    private final DataDirectoryPolicy dataDirectoryPolicy;
    private final PortPolicy clientPortPolicy;
    private final PortPolicy serverFirstPortPolicy;
    private final PortPolicy serverSecondPortPolicy;
    private final Collection<Runnable> shutdownHooks = new ArrayList<Runnable>();

    public ZooKeeperClusterLifecycle( int size, DataDirectoryPolicy dataDirectoryPolicy,
            PortPolicy clientPortPolicy, PortPolicy serverFirstPortPolicy,
            PortPolicy serverSecondPortPolicy ) throws IOException
    {
        this.size = size;
        this.dataDirectoryPolicy = dataDirectoryPolicy;
        this.clientPortPolicy = clientPortPolicy;
        this.serverFirstPortPolicy = serverFirstPortPolicy;
        this.serverSecondPortPolicy = serverSecondPortPolicy;
        startCluster();
    }
    
    private void startCluster() throws IOException
    {
        for ( int i = 0; i < size; i++ )
        {
            File configFile = writeZooKeeperConfigFile( i+1 );
            final Process process = Runtime.getRuntime().exec( new String[] { "java", "-cp",
                    System.getProperty( "java.class.path" ),
                    "org.apache.zookeeper.server.quorum.QuorumPeerMain",
                    configFile.getAbsolutePath() } );
            shutdownHooks.add( new Runnable()
            {
                public void run()
                {
                    process.destroy();
                }
            } );
        }
        waitForClusterToBeFullyStarted();
    }
    
    private void waitForClusterToBeFullyStarted()
    {
        try
        {
            Thread.sleep( 10000 );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
    }

    private File writeZooKeeperConfigFile( int id ) throws IOException
    {
        File directory = dataDirectoryPolicy.getDataDirectory( id );
        directory.mkdirs();
        File configFile = new File( directory, "config" + id + ".cfg" );
        Properties props = new Properties();
        populateZooConfig( props, id );
        FileWriter writer = null;
        writer = new FileWriter( configFile );
        for ( Object key : new TreeSet<Object>( props.keySet() ) )
        {
            writer.write( key + " = " + props.get( key ) + "\n" );
        }
        writer.close();
        
        writer = new FileWriter( new File( directory, "myid" ) );
        writer.write( "" + id );
        writer.close();
        return configFile;
    }
    
    private void populateZooConfig( Properties props, int id )
    {
        props.setProperty( "tickTime", "2000" );
        props.setProperty( "initLimit", "10" );
        props.setProperty( "syncLimit", "5" );
        props.setProperty( "clientPort", "" + clientPortPolicy.getPort( id ) );
        props.setProperty( "dataDir", dataDirectoryPolicy.getDataDirectory( id ).getPath() );
        for ( int i = 1; i <= size; i++ )
        {
            props.setProperty( "server." + i, "localhost:" + serverFirstPortPolicy.getPort( i ) +
                    ":" + serverSecondPortPolicy.getPort( i ) );
        }
    }
    
    public void shutdown()
    {
        for ( Runnable hook : shutdownHooks )
        {
            hook.run();
        }
    }

    public static interface DataDirectoryPolicy
    {
        File getDataDirectory( int id );
    }
    
    public static interface PortPolicy
    {
        int getPort( int id );
    }
    
    public static DataDirectoryPolicy defaultDataDirectoryPolicy( final File baseDirectory )
    {
        return new DataDirectoryPolicy()
        {
            public File getDataDirectory( int id )
            {
                return new File( baseDirectory, zeroLeadingId( id, 2 ) );
            }

            private String zeroLeadingId( int id, int minLength )
            {
                String result = "" + id;
                while ( result.length() < minLength )
                {
                    result = "0" + result;
                }
                return result;
            }
        };
    }
    
    public static PortPolicy defaultPortPolicy( final int startPort )
    {
        return new PortPolicy()
        {
            public int getPort( int id )
            {
                // Since id starts at 1
                return startPort + id - 1;
            }
        };
    }

    public int getSize()
    {
        return size;
    }

    public DataDirectoryPolicy getDataDirectoryPolicy()
    {
        return dataDirectoryPolicy;
    }

    public PortPolicy getClientPortPolicy()
    {
        return clientPortPolicy;
    }

    public PortPolicy getServerFirstPortPolicy()
    {
        return serverFirstPortPolicy;
    }

    public PortPolicy getServerSecondPortPolicy()
    {
        return serverSecondPortPolicy;
    }
}
