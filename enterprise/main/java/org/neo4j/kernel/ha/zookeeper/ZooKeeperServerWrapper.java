package org.neo4j.kernel.ha.zookeeper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

public class ZooKeeperServerWrapper
{
    private final int myId;
    private final File dataDirectory;
    private final int clientPort;
    private final Process process;
    private final Map<String, String> additionalConfig;
    private final Collection<String> allZooKeeperServers;

    public ZooKeeperServerWrapper( int myId, File dataDirectory, int clientPort,
            Collection<String> allZooKeeperServers, Map<String, String> additionalConfig )
            throws IOException
    {
        this.myId = myId;
        this.dataDirectory = dataDirectory;
        this.clientPort = clientPort;
        this.allZooKeeperServers = allZooKeeperServers;
        this.additionalConfig = additionalConfig;
        this.process = startServer();
    }

    private Process startServer() throws IOException
    {
        File configFile = createZooKeeperDirectory();
        return Runtime.getRuntime().exec( new String[] { "java", "-cp",
                System.getProperty( "java.class.path" ),
                "org.apache.zookeeper.server.quorum.QuorumPeerMain",
                configFile.getAbsolutePath() } );
    }

    private File createZooKeeperDirectory() throws IOException
    {
        dataDirectory.mkdirs();
        File configFile = new File( dataDirectory, "config.cfg" );
        Properties props = new Properties();
        populateZooConfig( props );
        FileWriter writer = null;
        writer = new FileWriter( configFile );
        for ( Object key : new TreeSet<Object>( props.keySet() ) )
        {
            writer.write( key + " = " + props.get( key ) + "\n" );
        }
        writer.close();
        
        writer = new FileWriter( new File( dataDirectory, "myid" ) );
        writer.write( "" + myId );
        writer.close();
        return configFile;
    }
    
    private void populateZooConfig( Properties props )
    {
        props.setProperty( "clientPort", "" + clientPort );
        props.setProperty( "dataDir", dataDirectory.getPath() );
        
        fillPropsOrDefault( props, "tickTime", "2000" );
        fillPropsOrDefault( props, "initLimit", "10" );
        fillPropsOrDefault( props, "syncLimit", "5" );
        
        int counter = 1;
        for ( String server : allZooKeeperServers )
        {
            props.setProperty( "server." + counter++, server );
        }
    }
    
    private void fillPropsOrDefault( Properties props, String key, String defaultValue )
    {
        String value = (additionalConfig != null && additionalConfig.containsKey( key )) ?
                additionalConfig.get( key ) : defaultValue;
        props.setProperty( key, value );
    }
    
    public void shutdown()
    {
        this.process.destroy();
    }
}
