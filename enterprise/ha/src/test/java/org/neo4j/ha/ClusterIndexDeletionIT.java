/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ClusterIndexDeletionIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( HaSettings.tx_push_factor, "2" )
            .withSharedSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );

    @Test
    public void givenClusterWithCreatedIndexWhenDeleteIndexOnMasterThenIndexIsDeletedOnSlave() throws Throwable
    {
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            master.index().forNodes( "Test" );
            tx.success();
        }

        cluster.sync();

        HighlyAvailableGraphDatabase aSlave = cluster.getAnySlave();
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

        cluster.sync();

        // Then
        HighlyAvailableGraphDatabase anotherSlave = cluster.getAnySlave();
        try ( Transaction tx = anotherSlave.beginTx() )
        {
            assertThat( anotherSlave.index().existsForNodes( "Test" ), equalTo( false ) );
            tx.success();
        }
    }
}
