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
package jmx;

import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.ClusterMemberInfo;
import org.neo4j.management.HighAvailability;
import org.neo4j.management.Neo4jManager;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.test.ha.ClusterRule.intBase;
import static org.neo4j.test.ha.ClusterRule.stringWithIntBase;

public class HaBeanIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withInstanceSetting( setting( "jmx.port", STRING, Settings.NO_DEFAULT ), intBase( 9912 ) )
            .withInstanceSetting( HaSettings.ha_server, stringWithIntBase( ":", 1136 ) )
            .withInstanceSetting( GraphDatabaseSettings.forced_kernel_id, stringWithIntBase( "kernel", 0 ) );

    @Test
    public void shouldAccessHaBeans() throws Throwable
    {
        ManagedCluster cluster = clusterRule.startCluster();

        // High Availability bean
        HighAvailability ha = ha( cluster.getMaster() );
        assertNotNull( "could not get ha bean", ha );
        assertMasterInformation( ha );
        assertMasterAndSlaveInformation( ha.getInstancesInCluster() );
        for ( ClusterMemberInfo info : ha.getInstancesInCluster() )
        {
            assertTrue( info.isAlive() );
            assertTrue( info.isAvailable() );
        }

        // Branched data bean
        BranchedStore bs = beans( cluster.getMaster() ).getBranchedStoreBean();
        assertNotNull( "could not get branched store bean", bs );
    }

    private void assertMasterInformation( HighAvailability ha )
    {
        assertTrue( "should be available", ha.isAvailable() );
        assertEquals( "should be master", HighAvailabilityModeSwitcher.MASTER, ha.getRole() );
    }

    private Neo4jManager beans( HighlyAvailableGraphDatabase db )
    {
        return new Neo4jManager( db.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) );
    }

    private HighAvailability ha( HighlyAvailableGraphDatabase db )
    {
        return beans( db ).getHighAvailabilityBean();
    }

    private static URI getUriForScheme( final String scheme, Iterable<URI> uris )
    {
        return firstOrNull( filter( item ->  item.getScheme().equals( scheme ), uris ) );
    }

    private void assertMasterAndSlaveInformation( ClusterMemberInfo[] instancesInCluster ) throws Exception
    {
        ClusterMemberInfo master = member( instancesInCluster, 1 );
        assertEquals( 1137, getUriForScheme( "ha", Iterables.map( URI::create, Arrays.asList( master.getUris() ) ) ).getPort() );
        assertEquals( HighAvailabilityModeSwitcher.MASTER, master.getHaRole() );

        ClusterMemberInfo slave = member( instancesInCluster, 2 );
        assertEquals( 1138, getUriForScheme( "ha", Iterables.map( URI::create, Arrays.asList( slave.getUris() ) ) ).getPort() );
        assertEquals( HighAvailabilityModeSwitcher.SLAVE, slave.getHaRole() );
        assertTrue( "Slave not available", slave.isAvailable() );
    }

    private ClusterMemberInfo member( ClusterMemberInfo[] members, int instanceId )
    {
        for ( ClusterMemberInfo member : members )
        {
            if ( member.getInstanceId().equals( Integer.toString( instanceId ) ) )
            {
                return member;
            }
        }
        fail( "Couldn't find cluster member with cluster URI port " + instanceId + " among " + Arrays.toString(
                members ) );
        return null; // it will never get here.
    }
}
