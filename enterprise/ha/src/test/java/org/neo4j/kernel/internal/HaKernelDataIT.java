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
package org.neo4j.kernel.internal;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.ManagedResource;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertSame;

public class HaKernelDataIT
{
    @Test
    public void shouldReturnHaGraphDbFromKernelData()
    {
        // given
        HighlyAvailableGraphDatabase haGraphDb = ha.getResource();
        KernelData kernelData = haGraphDb.getDependencyResolver().resolveDependency( KernelData.class );

        // then
        assertSame( kernelData.graphDatabase(), haGraphDb );
    }

    @Rule
    public final ManagedResource<HighlyAvailableGraphDatabase> ha = new ManagedResource<HighlyAvailableGraphDatabase>()
    {
        @Override
        protected HighlyAvailableGraphDatabase createResource( TestDirectory dir )
        {
            int clusterPort = PortAuthority.allocatePort();

            return (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                    newEmbeddedDatabaseBuilder( dir.directory().getAbsoluteFile() )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + clusterPort )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:" + clusterPort )
                    .setConfig( HaSettings.ha_server, "127.0.0.1:" + PortAuthority.allocatePort() )
                    .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
                    .newGraphDatabase();
        }

        @Override
        protected void disposeResource( HighlyAvailableGraphDatabase resource )
        {
            resource.shutdown();
        }
    };
}
