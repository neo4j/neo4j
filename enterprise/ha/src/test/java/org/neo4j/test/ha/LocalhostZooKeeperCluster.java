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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.zookeeper.server.quorum.QuorumMXBean;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.jboss.netty.handler.timeout.TimeoutException;
import org.junit.Ignore;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperClusterClient;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.subprocess.SubProcess;

import static java.lang.management.ManagementFactory.*;

@Ignore
public final class LocalhostZooKeeperCluster
{
    private final ZooKeeper[] keeper;
    private String connection;

    public LocalhostZooKeeperCluster( Class<?> owningTest, int... ports )
    {
        this( TargetDirectory.forTest( owningTest ), ports );
    }

    public LocalhostZooKeeperCluster( TargetDirectory target, int... ports )
    {
        keeper = new ZooKeeper[ports.length];
        boolean success = false;
        try
        {
            ZooKeeperProcess subprocess = new ZooKeeperProcess( null );
            StringBuilder connection = new StringBuilder();
            for ( int i = 0; i < keeper.length; i++ )
            {
                keeper[i] = subprocess.start( new String[] { config( target, i + 1, ports[i] ) } );
                if ( connection.length() > 0 ) connection.append( "," );
                connection.append( "localhost:" + ports[i] );
            }
            this.connection = connection.toString();
            await( keeper, 15, TimeUnit.SECONDS );
            success = true;
        } catch (Throwable ex)
        {
            ex.printStackTrace(  );
        }
        finally
        {
            if ( !success ) shutdown();
        }
    }

    public static LocalhostZooKeeperCluster standardZoo( Class<?> owningTest)
    {
        return new LocalhostZooKeeperCluster( owningTest, 2181, 2182, 2183 );
    }

    private void await( ZooKeeper[] keepers, long timeout, TimeUnit unit )
    {
        timeout = System.currentTimeMillis() + unit.toMillis( timeout );
        do
        {
            ZooKeeperClusterClient cm = null;
            try
            {
                cm = new ZooKeeperClusterClient( getConnectionString(),
                                                 ConfigurationDefaults.getDefault( HaSettings.cluster_name, HaSettings.class ));
                cm.waitForSyncConnected();
                break;
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                // ok retry
            }
            finally
            {
                if ( cm != null )
                {
                    try
                    {
                        cm.shutdown();
                    }
                    catch ( Throwable t )
                    {
                        t.printStackTrace();
                    }
                }
            }
            if ( System.currentTimeMillis() > timeout )
            {
                throw new TimeoutException( "waiting for ZooKeeper cluster to start" );
            }
            try
            {
                Thread.sleep( 2000 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
        while ( true );
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
        for ( ZooKeeper zk : keeper )
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
        if ( keeper.length > 0 && keeper[0] == null ) return;
        for ( ZooKeeper zk : keeper )
        {
            if ( zk != null ) SubProcess.stop( zk );
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

    public interface ZooKeeper
    {
        int getQuorumSize();

        String getStatus();
    }

    private static class ZooKeeperProcess extends SubProcess<ZooKeeper, String[]> implements
            ZooKeeper
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
