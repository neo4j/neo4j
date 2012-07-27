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
package org.neo4j.kernel;

import java.util.HashMap;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.test.ManagedResource;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

import static org.junit.Assert.assertSame;

@Ignore("We want to tie the thing owning the instanceId to the HA db, but the extensions need access to the " +
                "internal db through the KernelData object while starting the internal db, " +
                "making for an impossible circular reference")
public class HaKernelDataTest
{
    @Test
    public void shouldReturnHaGraphDbFromKernelData() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase haGraphDb = ha.getResource();
        KernelData kernelData = haGraphDb.getKernelData();

        // then
        assertSame( kernelData.graphDatabase(), haGraphDb );
    }

    @Test
    public void shouldNotInstantiateNewKernelDataOnInternalRestart() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase haGraphDb = ha.getResource();
        KernelData kernelData = haGraphDb.getKernelData();

        // when -- force internal restart
        haGraphDb.internalShutdown( false );
        haGraphDb.reevaluateMyself();

        // then
        assertSame( kernelData, haGraphDb.getKernelData() );
    }

    @Rule
    public final ManagedResource<HighlyAvailableGraphDatabase> ha = new ManagedResource<HighlyAvailableGraphDatabase>()
    {
        @Override
        protected HighlyAvailableGraphDatabase createResource( TargetDirectory.TestDirectory dir ) throws Exception
        {
            HashMap<String, String> config = new HashMap<String, String>();
            config.put( HaSettings.server_id.name(), "1" );
            config.put( HaSettings.coordinators.name(), zkConnection() );
            return new HighlyAvailableGraphDatabase( dir.directory().getAbsolutePath(), config );
        }

        private String zkConnection() throws Exception
        {
            return LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection().getConnectionString();
        }

        @Override
        protected void disposeResource( HighlyAvailableGraphDatabase resource )
        {
            resource.shutdown();
        }
    };
}
