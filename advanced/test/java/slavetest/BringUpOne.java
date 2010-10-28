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
import java.util.Date;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.zookeeper.NeoStoreUtil;

public class BringUpOne
{
    public static void main( String[] args ) throws Exception
    {
        File path = new File( "test/dbs/0" );
        NeoStoreUtil store = new NeoStoreUtil( path.getPath() );
        System.out.println( "Starting store: createTime=" + new Date( store.getCreationTime() ) +
                " identifier=" + store.getStoreId() + " last committed tx=" + store.getLastCommittedTx() );
        final GraphDatabaseService db = new HighlyAvailableGraphDatabase( path.getPath(), MapUtil.stringMap(
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, "1",
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS, "localhost:2181,localhost:2182,localhost:2183",
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVER, "localhost:5559",
                Config.ENABLE_REMOTE_SHELL, "true",
                Config.KEEP_LOGICAL_LOGS, "true" ) );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            public void run()
            {
                db.shutdown();
            }
        } );
        System.out.println( "up" );
        while ( true ) Thread.sleep( 1000 );
    }
}
