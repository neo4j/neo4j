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

import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.test.ha.ClusterManager.fromXml;

public class TestClusterIndexDeletion
{
    @Test
    public void givenClusterWithCreatedIndexWhenDeleteIndexOnMasterThenIndexIsDeletedOnSlave() throws Throwable
    {
        ClusterManager clusterManager =
            new ClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).cleanDirectory( "testCluster" ),
                MapUtil.stringMap( HaSettings.ha_server.name(), ":6001-6005",
                        HaSettings.tx_push_factor.name(), "2" ));
        try
        {
            // Given
            clusterManager.start();

            clusterManager.getDefaultCluster().await( ClusterManager.allSeesAllAsAvailable() );

            GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
            try ( Transaction tx = master.beginTx() )
            {
                master.index().forNodes( "Test" );
                tx.success();
            }

            HighlyAvailableGraphDatabase aSlave = clusterManager.getDefaultCluster().getAnySlave();
            try ( Transaction tx = aSlave.beginTx() )
            {
                assertThat( aSlave.index().existsForNodes( "Test" ), equalTo( true ) );
                tx.success();
            }

            // When
            try ( Transaction tx = master.beginTx() )
            {
                master.index().forNodes( "Test" ).delete();
                tx.success();
            }

            // Then
            HighlyAvailableGraphDatabase anotherSlave = clusterManager.getDefaultCluster().getAnySlave();
            try ( Transaction tx = anotherSlave.beginTx() )
            {
                assertThat( anotherSlave.index().existsForNodes( "Test" ), equalTo( false ) );
                tx.success();
            }
        }
        finally
        {
            clusterManager.stop();
        }
    }

}
