package slavetest;

import java.io.File;

public class BringUpZooKeeper
{
    public static void main( String[] args ) throws Exception
    {
        final LocalZooKeeperCluster zoo = new LocalZooKeeperCluster( 3,
                LocalZooKeeperCluster.defaultDataDirectoryPolicy( new File( "test/zoo" ) ),
                LocalZooKeeperCluster.defaultPortPolicy( 2181 ),
                LocalZooKeeperCluster.defaultPortPolicy( 2888 ),
                LocalZooKeeperCluster.defaultPortPolicy( 3888 ) );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                zoo.shutdown();
            }
        } );
        
        System.out.println( "Zoo keeper cluster up" );
        
        while ( true ) Thread.sleep( 1000 );
    }
}
