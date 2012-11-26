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
package org.neo4j.kernel.ha;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.consistency.checking.incremental.intercept.VerifyingTransactionInterceptorProvider;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.test.TargetDirectory;

public class TestMasterElection
{
    private final File path = TargetDirectory.forTest( getClass() ).graphDbDir( true );

    @Test
    public void testBasicFailover() throws Exception
    {
        HighlyAvailableGraphDatabase master = startDb( 0 );
        HighlyAvailableGraphDatabase slave1 = startDb( 1 );
        HighlyAvailableGraphDatabase slave2 = startDb( 2 );

        assertTrue( master.isMaster() );
        assertTrue( !slave1.isMaster() );
        assertTrue( !slave2.isMaster() );

        startListenForNewMaster( slave2 );

        master.shutdown();
        assertTrue( masterElectedLatch.await( 20, SECONDS ) );
        assertTrue( slave1.isMaster() );
        assertTrue( !slave2.isMaster() );

        slave2.shutdown();
        slave1.shutdown();
    }

    private void startListenForNewMaster( HighlyAvailableGraphDatabase db )
    {
        masterElectedLatch = new CountDownLatch( 1 );
        final ClusterMemberEvents events = db.getDependencyResolver().resolveDependency( ClusterMemberEvents.class );
        events.addClusterMemberListener( new ClusterMemberListener.Adapter()
        {
            @Override
            public void memberIsAvailable( String role, URI instanceClusterUri, URI roleUri )
            {
                if ( role.equals( HighAvailabilityModeSwitcher.MASTER ) )
                {
                    masterElectedLatch.countDown();
                    events.removeClusterMemberListener( this );
                }
            }
        } );
    }

    private CountDownLatch masterElectedLatch;

    private HighlyAvailableGraphDatabase startDb( int serverId )
    {
        GraphDatabaseBuilder builder = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( path( serverId ) )
                .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003" )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + (5001 + serverId) )
                .setConfig( HaSettings.server_id, "" + serverId )
                .setConfig( HaSettings.ha_server, ":" + (8001 + serverId) )
                .setConfig( HaSettings.tx_push_factor, "0" )
                .setConfig( GraphDatabaseSettings.intercept_committing_transactions, "true" )
                .setConfig( GraphDatabaseSettings.intercept_deserialized_transactions, "true" )
                .setConfig(TransactionInterceptorProvider.class.getSimpleName() + "." +
                        VerifyingTransactionInterceptorProvider.NAME, "true" )
                ;
        return (HighlyAvailableGraphDatabase) builder.newGraphDatabase();
    }

    private String path( int i )
    {
        return new File( path, "" + i ).getAbsolutePath();
    }
}
