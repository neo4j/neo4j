/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.ha;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.junit.Ignore;
import org.neo4j.test.SubProcess;
import org.neo4j.test.TargetDirectory;

@Ignore
public final class LocalhostZooKeeperCluster
{
    private final Object[] keeper;
    private final String connection;

    public LocalhostZooKeeperCluster( Class<?> owningTest, int... ports )
    {
        this( TargetDirectory.forTest( owningTest ), ports );
    }

    public LocalhostZooKeeperCluster( TargetDirectory target, int... ports )
    {
        keeper = new Object[ports.length];
        ZooKeeperProcess subprocess = new ZooKeeperProcess();
        StringBuilder connection = new StringBuilder();
        for ( int i = 0; i < keeper.length; i++ )
        {
            keeper[i] = subprocess.start( new String[] { config( target, i + 1, ports[i] ) } );
            if ( connection.length() > 0 ) connection.append( "," );
            connection.append( "localhost:" + ports[i] );
        }
        this.connection = connection.toString();
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

    private String config( TargetDirectory target, int id, int port )
    {
        File config = target.file( "zookeeper" + id + ".cfg" );
        File dataDir = target.directory( "zk" + id + "data" );
        try
        {
            PrintWriter conf = new PrintWriter( config );
            try
            {
                conf.println( "tickTime=2000" );
                conf.println( "initLimit=10" );
                conf.println( "syncLimit=5" );
                conf.println( "dataDir=" + dataDir.getAbsolutePath() );
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
        for ( Object zk : keeper )
        {
            SubProcess.stop( zk );
        }
        Arrays.fill( keeper, null );
    }

    private static class ZooKeeperProcess extends SubProcess<Object, String[]>
    {
        @Override
        protected void startup( String[] parameters )
        {
            System.out.println( "parameters=" + Arrays.toString( parameters ) );
            QuorumPeerMain.main( parameters );
        }
    }
}
