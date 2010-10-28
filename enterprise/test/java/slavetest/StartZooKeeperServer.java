/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package slavetest;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;

import org.neo4j.kernel.ha.zookeeper.ZooKeeperServerWrapper;

public class StartZooKeeperServer
{
    static final int CLIENT_PORT = 2181;
    static final String[] ZOO_KEEPER_SERVERS = new String[] {
            "172.16.2.33:" + CLIENT_PORT,
            "172.16.1.242:" + CLIENT_PORT,
            "172.16.4.14:" + CLIENT_PORT
    };
    
    public static void main( String[] args ) throws Exception
    {
        final ZooKeeperServerWrapper zooKeeperServer = new ZooKeeperServerWrapper( 2,
                new File( "var/zookeeper" ), CLIENT_PORT, Arrays.asList( ZOO_KEEPER_SERVERS ),
                new HashMap<String, String>() );
        System.out.println( "Zoo keeper server up, stop it with Ctrl-C" );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                System.out.print( "Shutting down zoo keeper server... " );
                zooKeeperServer.shutdown();
                System.out.println( "OK" );
            }
        } );
        
        while ( true )
        {
            Thread.sleep( 1000 );
        }
    }
}
