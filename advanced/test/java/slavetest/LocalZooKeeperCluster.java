package slavetest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.kernel.ha.zookeeper.ZooKeeperServerWrapper;

public class LocalZooKeeperCluster
{
    private final int size;
    private final DataDirectoryPolicy dataDirectoryPolicy;
    private final PortPolicy clientPortPolicy;
    private final PortPolicy serverFirstPortPolicy;
    private final PortPolicy serverSecondPortPolicy;
    private final Collection<ZooKeeperServerWrapper> wrappers =
            new ArrayList<ZooKeeperServerWrapper>();

    public LocalZooKeeperCluster( int size, DataDirectoryPolicy dataDirectoryPolicy,
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
        Collection<String> servers = new ArrayList<String>();
        for ( int i = 0; i < size; i++ )
        {
            int id = i+1;
            servers.add( "localhost:" + serverFirstPortPolicy.getPort( id ) + ":" +
                    serverSecondPortPolicy.getPort( id ) );
        }
        
        for ( int i = 0; i < size; i++ )
        {
            int id = i+1;
            ZooKeeperServerWrapper wrapper = new ZooKeeperServerWrapper( id,
                    dataDirectoryPolicy.getDataDirectory( id ),
                    clientPortPolicy.getPort( id ), servers, Collections.<String, String>emptyMap() );
            wrappers.add( wrapper );
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
    
    public void shutdown()
    {
        for ( ZooKeeperServerWrapper wrapper : wrappers )
        {
            wrapper.shutdown();
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
