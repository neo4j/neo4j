/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;

import java.io.File;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore
public class ConflictingServerIdIT
{
    private static final File path = TargetDirectory.forTest( ConflictingServerIdIT.class ).graphDbDir( true );

    @Test
    public void testConflictingIdDoesNotSilentlyFail() throws Exception
    {
        HighlyAvailableGraphDatabase master = null, dbWithId21 = null, dbWithId22 = null;
        try
        {
            GraphDatabaseBuilder masterBuilder = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( path( 1 ) )
                .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004" )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + ( 5001 + 1 ) )
                .setConfig( HaSettings.server_id, "" + 1 )
                .setConfig( HaSettings.ha_server, ":" + ( 8001 + 1 ) )
                .setConfig( HaSettings.tx_push_factor, "0" );
            master = (HighlyAvailableGraphDatabase) masterBuilder.newGraphDatabase();

            GraphDatabaseBuilder db21Builder = new HighlyAvailableGraphDatabaseFactory()
                    .newHighlyAvailableDatabaseBuilder( path( 2 ) )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + ( 5001 + 2 ) )
                    .setConfig( HaSettings.server_id, "" + 2 )
                    .setConfig( HaSettings.ha_server, ":" + ( 8001 + 2 ) )
                    .setConfig( HaSettings.tx_push_factor, "0" );
            dbWithId21 = (HighlyAvailableGraphDatabase) db21Builder.newGraphDatabase();

            GraphDatabaseBuilder db22Builder = new HighlyAvailableGraphDatabaseFactory()
                    .newHighlyAvailableDatabaseBuilder( path( 3 ) )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + (5001 + 3) )
                    .setConfig( HaSettings.server_id, "" + 2 ) // Conflicting with the above
                    .setConfig( HaSettings.ha_server, ":" + ( 8001 + 3 ) )
                    .setConfig( HaSettings.tx_push_factor, "0" );
            dbWithId22 = (HighlyAvailableGraphDatabase) db22Builder.newGraphDatabase();

            assertTrue( master.isMaster() );
            assertTrue( !dbWithId21.isMaster() );
            assertTrue( !dbWithId22.isMaster() );

            Transaction tx1 = dbWithId21.beginTx(  );
            tx1.success();
            tx1.finish();

            try
            {
                Transaction tx = dbWithId22.beginTx(  );
                tx.success();
                tx.finish();
                fail( "Should not be able to do txs on instance with conflicting serverId" );
            }
            catch ( TransactionFailureException e )
            {
                // happy happy happy
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

    private static String path( int i )
    {
        return new File( path, "" + i ).getAbsolutePath();
    }
}
