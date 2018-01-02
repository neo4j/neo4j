/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConflictingServerIdIT
{
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void testConflictingIdDoesNotSilentlyFail() throws Exception
    {
        HighlyAvailableGraphDatabase master = null, dbWithId21 = null, dbWithId22 = null;
        try
        {

            GraphDatabaseBuilder masterBuilder = new TestHighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( path( 1 ) )
                .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5002" )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + ( 5001 + 1 ) )
                .setConfig( ClusterSettings.server_id, "" + 1 )
                .setConfig( HaSettings.ha_server, ":" + ( 8001 + 1 ) )
                .setConfig( HaSettings.tx_push_factor, "0" );
            master = (HighlyAvailableGraphDatabase) masterBuilder.newGraphDatabase();

            GraphDatabaseBuilder db21Builder = new TestHighlyAvailableGraphDatabaseFactory()
                    .newHighlyAvailableDatabaseBuilder( path( 2 ) )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5002,127.0.0.1:5003" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + ( 5001 + 2 ) )
                    .setConfig( ClusterSettings.server_id, "" + 2 )
                    .setConfig( HaSettings.ha_server, ":" + ( 8001 + 2 ) )
                    .setConfig( HaSettings.tx_push_factor, "0" );
            dbWithId21 = (HighlyAvailableGraphDatabase) db21Builder.newGraphDatabase();

            GraphDatabaseBuilder db22Builder = new TestHighlyAvailableGraphDatabaseFactory()
                    .newHighlyAvailableDatabaseBuilder( path( 3 ) )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5002" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + (5001 + 3) )
                    .setConfig( ClusterSettings.server_id, "" + 2 ) // Conflicting with the above
                    .setConfig( HaSettings.ha_server, ":" + ( 8001 + 3 ) )
                    .setConfig( HaSettings.tx_push_factor, "0" );

            try
            {
                dbWithId22 = (HighlyAvailableGraphDatabase) db22Builder.newGraphDatabase();
                fail("Should not be able to startup when a cluster already has my id");
            }
            catch ( Exception e )
            {
                // awesome
            }

            assertTrue( master.isMaster() );
            assertTrue( !dbWithId21.isMaster() );

            try ( Transaction transaction = dbWithId21.beginTx() )
            {
                transaction.success();
            }
        }
        finally
        {
            if ( dbWithId21 != null )
            {
                dbWithId21.shutdown();
            }
            if ( dbWithId22 != null )
            {
                dbWithId22.shutdown();
            }
            if ( master != null )
            {
                master.shutdown();
            }
        }
    }

    private String path( int i )
    {
        return new File( testDirectory.graphDbDir(), "" + i ).getAbsolutePath();
    }
}
