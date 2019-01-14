/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
    public ClusterRule clusterRule = new ClusterRule()
            .withSharedSetting( HaSettings.tx_push_factor, "2" )
            .withSharedSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );

    @Test
    public void givenClusterWithCreatedIndexWhenDeleteIndexOnMasterThenIndexIsDeletedOnSlave()
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
