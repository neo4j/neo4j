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
package jmx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.neo4j.ha.CreateEmptyDb;
import org.neo4j.ha.Neo4jHaCluster;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.jmx.Kernel;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.management.HighAvailability;
import org.neo4j.management.InstanceInfo;
import org.neo4j.management.Neo4jManager;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestHaBean
{
    private static final TargetDirectory dir = TargetDirectory.forTest( TestHaBean.class );
    private static LocalhostZooKeeperCluster zk;
    private static HighlyAvailableGraphDatabase db;

    @BeforeClass
    public static void startDb() throws Exception
    {
        zk = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
        File storeDir = dir.graphDbDir( /*clean=*/true );
        CreateEmptyDb.at( storeDir );
        db = Neo4jHaCluster.single( zk, storeDir, /*HA port:*/3377, //
                MapUtil.stringMap( "jmx.port", "9913" ) );
    }

    @AfterClass
    public static void stopDb()
    {
        if ( db != null ) db.shutdown();
        db = null;
        dir.cleanup();
    }

    @Test
    public void canGetHaBean() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager( db.getManagementBean( Kernel.class ) );
        HighAvailability ha = neo4j.getHighAvailabilityBean();
        assertNotNull( "could not get ha bean", ha );
        assertTrue( "single instance should be master", ha.isMaster() );
    }

    @Test
    @Ignore //Temporary ignore since this doesn't work well on Linux 2011-04-08
    public void canGetInstanceConnectionInformation() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager( db.getManagementBean( Kernel.class ) );
        InstanceInfo[] instances = neo4j.getHighAvailabilityBean().getInstancesInCluster();
        assertNotNull( instances );
        assertEquals( 1, instances.length );
        InstanceInfo instance = instances[0];
        assertNotNull( instance );
        String address = instance.getAddress();
        assertNotNull( "No JMX address for instance", address );
        String id = instance.getInstanceId();
        assertNotNull( "No instance id", id );
    }

    @Test
    @Ignore //Temporary ignore since this doesn't work well on Linux 2011-04-08
    public void canConnectToInstance() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager( db.getManagementBean( Kernel.class ) );
        HighAvailability ha = neo4j.getHighAvailabilityBean();
        InstanceInfo[] instances = ha.getInstancesInCluster();
        assertNotNull( instances );
        assertEquals( 1, instances.length );
        InstanceInfo instance = instances[0];
        assertNotNull( instance );
        Pair<Neo4jManager, HighAvailability> proc = instance.connect();
        assertNotNull( "could not connect", proc );
        neo4j = proc.first();
        ha = proc.other();
        assertNotNull( neo4j );
        assertNotNull( ha );

        instances = ha.getInstancesInCluster();
        assertNotNull( instances );
        assertEquals( 1, instances.length );
        assertEquals( instance.getAddress(), instances[0].getAddress() );
        assertEquals( instance.getInstanceId(), instances[0].getInstanceId() );
    }
}
