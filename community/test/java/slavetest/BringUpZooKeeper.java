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
