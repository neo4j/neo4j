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
package slavetest.manual;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;



class LocalZooKeeperCluster
{
    private final int size;
    private final DataDirectoryPolicy dataDirectoryPolicy;
    private final PortPolicy clientPortPolicy;
    private final PortPolicy serverFirstPortPolicy;
    private final PortPolicy serverSecondPortPolicy;
    private final Collection<ZooKeeperServerWrapper> wrappers =
            new ArrayList<ZooKeeperServerWrapper>();
    private Collection<String> serversConfig;

    public LocalZooKeeperCluster( int size, File baseDir ) throws IOException
    {
        this( size, defaultDataDirectoryPolicy( baseDir ),
                defaultPortPolicy( 2181 ), defaultPortPolicy( 2888 ), defaultPortPolicy( 3888 ) );
    }

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
        this.serversConfig = servers;

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

    public Collection<String> getZooKeeperServersConfig()
    {
        return serversConfig;
    }

    public String getZooKeeperServersForHaInstance()
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < size; i++ )
        {
            builder.append( (i > 0 ? "," : "") + "localhost:" + clientPortPolicy.getPort( i+1 ) );
        }
        return builder.toString();
    }

    private void waitForClusterToBeFullyStarted()
    {
        try
        {
            Thread.sleep( 5000 );
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
