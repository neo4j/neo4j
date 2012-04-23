/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.test.ha;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumMXBean;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.junit.Ignore;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.Predicate;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.subprocess.SubProcess;

@Ignore
public final class LocalhostZooKeeperCluster
{
    private static LocalhostZooKeeperCluster SINGLETON;
    
    public static synchronized LocalhostZooKeeperCluster singleton()
    {
        if ( SINGLETON == null )
        {
            SINGLETON = new LocalhostZooKeeperCluster( LocalhostZooKeeperCluster.class, 2181 );
            Runtime.getRuntime().addShutdownHook( new Thread()
            {
                @Override
                public void run()
                {
                    SINGLETON.shutdown();
                }
            } );
        }
        return SINGLETON;
    }
    
    private final String connection;
    private volatile ZooKeeperMember[] keeper;
    private final int[] ports;
    private final TargetDirectory target;

    public LocalhostZooKeeperCluster( Class<?> owningTest, int... ports )
    {
        this( TargetDirectory.forTest( owningTest ), ports );
    }

    public LocalhostZooKeeperCluster( TargetDirectory target, int... ports )
    {
        this.target = target;
        this.ports = ports;
        connection = formConnectionString( ports );
        instantiateQuorumWithRetry();
    }

    private void instantiateQuorumWithRetry()
    {
        Exception error = null;
        for ( int i = 0; i < 3; i++ )
        {
            try
            {
                instantiateQuorum();
                return;
            }
            catch ( Exception e )
            {
                System.out.println( "ZK connection couldn't be verified, retrying..." );
                error = e;
            }
        }
        throw new RuntimeException( "Couldn't form a ZK quorum even after a couple of retries", error );
    }

    private String formConnectionString( int... ports )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < ports.length; i++ )
            builder.append( i > 0 ? "," : "" ).append( "localhost:" + ports[i] );
        return builder.toString();
    }

    private void instantiateQuorum()
    {
        keeper = new ZooKeeperMember[ports.length];
        boolean success = false;
        try
        {
            ZooKeeperProcess subprocess = new ZooKeeperProcess( null );
            for ( int i = 0; i < keeper.length; i++ )
                keeper[i] = subprocess.start( new String[] { config( target, i + 1, ports[i] ) } );
            verify( null );
            success = true;
        }
        catch ( Exception e )
        {
            throw Exceptions.launderedException( e );
        }
        finally
        {
            if ( !success )
                shutdown();
        }
    }
    
    public LocalhostZooKeeperCluster clearDataAndVerifyConnection() throws Exception
    {
        try
        {
            verify( new ZooKeeperJob()
            {
                @Override
                public void run( ZooKeeper client ) throws Exception
                {
                    for ( String child : client.getChildren( "/", false ) )
                    {
                        if ( child.equals( "zookeeper" ) )
                            continue;
                        deleteRecursively( client, "/" + child );
                    }
                }

                private void deleteRecursively( ZooKeeper client, String path ) throws Exception
                {
                    List<String> children = client.getChildren( path, false );
                    if ( children != null && !children.isEmpty() )
                        for ( String child : children )
                            deleteRecursively( client, path + "/" + child );
                    client.delete( path, -1 );
                }
            } );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            shutdown();
            instantiateQuorumWithRetry();
        }
        return this;
    }

    public static LocalhostZooKeeperCluster standardZoo( Class<?> owningTest)
    {
        return new LocalhostZooKeeperCluster( owningTest, 2181, 2182, 2183 );
    }

    private void verify( ZooKeeperJob job ) throws Exception
    {
        ZooKeeperAccess verifier = new ZooKeeperAccess( job );
        try
        {
            verifier.awaitSyncConnected( 15000 );
        }
        finally
        {
            verifier.close();
        }
    }
    
    private class ZooKeeperAccess implements Watcher
    {
        private final ZooKeeper keeper;
        private volatile boolean connected;
        private final ZooKeeperJob executeOnSyncConnected;
        
        ZooKeeperAccess( ZooKeeperJob executeOnSyncConnected ) throws IOException
        {
            this.executeOnSyncConnected = executeOnSyncConnected;
            this.keeper = new ZooKeeper( connection, 5000, this );
        }
        
        @Override
        public synchronized void process( WatchedEvent event )
        {
            switch ( event.getState() )
            {
            case SyncConnected:
                connected = true;
                notify();
                break;
            default: connected = false;
            }
        }

        public void close() throws Exception
        {
            keeper.close();
        }

        public synchronized void awaitSyncConnected( long timeout ) throws Exception
        {
            if ( !connected )
            {
                wait( timeout );
                if ( connected && executeOnSyncConnected != null )
                {
                    keeper.sync( "/", null, null );
                    executeOnSyncConnected.run( keeper );
                }
            }
            if ( !connected )
                throw new RuntimeException( "Connection not valid" );
        }
    }
    
    private interface ZooKeeperJob
    {
        void run( ZooKeeper client ) throws Exception;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + getConnectionString() + "]";
    }

    public synchronized String getConnectionString()
    {
        return connection;
    }

    String getStatus()
    {
        StringBuilder result = new StringBuilder();
        String prefix = "";
        for ( ZooKeeperMember zk : keeper )
        {
            result.append( prefix ).append( zk ).append( ": " ).append( zk.getStatus() );
            prefix = "; ";
        }
        return result.toString();
    }

    private String config( TargetDirectory target, int id, int port )
    {
        File config = target.file( "zookeeper" + id + ".cfg" );
        File dataDir = target.directory( "zk" + id + "data", true );
        try
        {
            PrintWriter conf = new PrintWriter( config );
            try
            {
                conf.println( "tickTime=2000" );
                conf.println( "initLimit=10" );
                conf.println( "syncLimit=5" );
                conf.println( "maxClientCnxns=50" );

                // On Windows the backslashes will have to be escaped for
                // ZooKeeper to interpret them correctly.
                conf.println( "dataDir=" + dataDir.getAbsolutePath().replaceAll( "\\\\", "\\\\\\\\" ) );

                conf.println( "clientPort=" + port );
                for ( int j = 0; j < keeper.length; j++ )
                {
                    conf.println( "server." + ( j + 1 ) + "=localhost:" + ( 2888 + j ) + ":"
                                  + ( 3888 + j ) );
                }
            }
            finally
            {
                conf.close();
            }
            PrintWriter myid = new PrintWriter( new File( dataDir, "myid" ) );
            try
            {
                myid.println( Integer.toString( id ) );
            }
            finally
            {
                myid.close();
            }
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Could not write ZooKeeper configuration", e );
        }
        return config.getAbsolutePath();
    }

    public synchronized void shutdown()
    {
        if ( keeper.length > 0 && keeper[0] == null )
            return;
        Thread[] stopperThreads = new Thread[keeper.length];
        for ( int i = 0; i < keeper.length; i++ )
        {
            final ZooKeeperMember member = keeper[i];
            stopperThreads[i] = new Thread()
            {
                @Override
                public void run()
                {
                    if ( member != null )
                        SubProcess.stop( member );
                }
            };
            stopperThreads[i].start();
        }
        
        for ( Thread thread : stopperThreads )
        {
            try
            {
                thread.join();
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
        
        Arrays.fill( keeper, null );
    }

    public static void main( String[] args ) throws Exception
    {
        LocalhostZooKeeperCluster cluster = standardZoo( LocalhostZooKeeperCluster.class );
        try
        {
            System.out.println( "Press return to exit" );
            System.in.read();
            System.out.println( "Shutting down cluster" );
        }
        finally
        {
            cluster.shutdown();
        }
    }

    public interface ZooKeeperMember
    {
        int getQuorumSize();

        String getStatus();
    }

    private static class ZooKeeperProcess extends SubProcess<ZooKeeperMember, String[]> implements ZooKeeperMember
    {
        private final String name;

        ZooKeeperProcess( String name )
        {
            super( new Predicate<String>()
            {
                @Override
                public boolean accept( String item )
                {
                    // ZooKeeper needs log4j proper,
                    // make sure slf4j doesn't get in the way
                    return !item.contains( "slf4j" );
                }
            } );
            this.name = name;
        }

        @Override
        protected void startup( String[] parameters )
        {
            System.out.println( "parameters=" + Arrays.toString( parameters ) );
            QuorumPeerMain.main( parameters );
        }

        @Override
        public String toString()
        {
            if ( name != null )
            {
                return super.toString() + ":" + name;
            }
            else
            {
                return super.toString();
            }
        }

        public int getQuorumSize()
        {
            try
            {
                return quorumBean().getQuorumSize();
            }
            catch ( Exception e )
            {
                return 0;
            }
        }

        public String getStatus()
        {
            try
            {
                return status( quorumBean() );
            }
            catch ( Exception e )
            {
                return "-down-";
            }
        }

        private QuorumMXBean quorumBean() throws MalformedObjectNameException
        {
            Set<ObjectName> names = getPlatformMBeanServer().queryNames(
                    new ObjectName( "org.apache.ZooKeeperService:name0=ReplicatedServer_id*" ),
                    null );
            QuorumMXBean quorum = MBeanServerInvocationHandler.newProxyInstance(
                    getPlatformMBeanServer(), names.iterator().next(), QuorumMXBean.class, false );
            return quorum;
        }

        @SuppressWarnings( "boxing" )
        private String status( QuorumMXBean quorumBean )
        {
            long time = System.currentTimeMillis();
            String name = quorumBean.getName();
            int size = quorumBean.getQuorumSize();
            return String.format( "name=%s, size=%s, time=%s (+%sms)", name, size, Format.time( time ),
                    System.currentTimeMillis() - time );
        }
    }
}
