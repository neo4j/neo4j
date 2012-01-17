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

import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class ManualZooKeepers
{
    public static final File PATH = new File( "var/zoo" );

    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Starting zoo keeper cluster (takes a couple of seconds)..." );
        final LocalhostZooKeeperCluster zoo = new LocalhostZooKeeperCluster( ManualZooKeepers.class, 2181, 2182, 2183 );
        System.out.println( "Zoo keeper cluster started, awaiting ENTER" );
        System.out.println( zoo.getConnectionString() );
        System.in.read();
        zoo.shutdown();
    }
}
