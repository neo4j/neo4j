/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.test.ha.ClusterManager;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;
import static org.neo4j.test.ha.ClusterManager.fromXml;

public class ClusterTransactionTest
{
    @Test
    public void givenClusterWhenShutdownMasterThenCannotStartTransactionOnSlave() throws Throwable
    {
        // Given
        ClusterManager clusterManager = new ClusterManager(
                fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                forTest( getClass() ).cleanDirectory( "testCluster" ),
                stringMap( HaSettings.ha_server.name(), ":6001-6005", HaSettings.tx_push_factor.name(), "2" ) );
        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( ClusterManager.allSeesAllAsAvailable() );

            GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
            final GraphDatabaseAPI slave = clusterManager.getDefaultCluster().getAnySlave();

            // When
            final FutureTask<Boolean> result = new FutureTask<>( new Callable<Boolean>()
            {
                @Override
                public Boolean call() throws Exception
                {
                    try ( Transaction tx = slave.beginTx() )
                    {
                        tx.acquireWriteLock( slave.getNodeById( 0 ) );
                        // Fail
                        return false;
                    }
                    catch ( Exception e )
                    {
                        // Ok!
                        return true;
                    }
                }
            } );
            master.getDependencyResolver()
                    .resolveDependency( LifeSupport.class )
                    .addLifecycleListener( new LifecycleListener()
                    {
                        @Override
                        public void notifyStatusChanged( Object instance, LifecycleStatus from, LifecycleStatus to )
                        {
                            if ( instance.getClass().getName().contains( "DatabaseAvailability" ) &&
                                    to == LifecycleStatus.STOPPED )
                            {
                                result.run();
                            }
                        }
                    } );

            master.shutdown();

            // Then
            assertThat( result.get(), equalTo( true ) );
        }
        finally
        {
            clusterManager.stop();
        }
    }
}
