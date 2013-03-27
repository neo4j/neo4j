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
package org.neo4j.cluster.protocol.heartbeat;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterContext;

/**
 * Tests basic sanity and various scenarios for HeartbeatContext.
 * All tests are performed from the perspective of instance running at 5001.
 */
public class HeartbeatContextTest
{
    private static InstanceId[] instanceIds = new InstanceId[]{
            new InstanceId( 1 ),
            new InstanceId( 2 ),
            new InstanceId( 3 )
    };

    private static String[] initialHosts = new String[]{
            "cluster://localhost:5001",
            "cluster://localhost:5002",
            "cluster://localhost:5003"
    };

    private HeartbeatContext toTest;
    private ClusterContext context;
//
//    @Before
//    public void setup()
//    {
//        Map<InstanceId, URI> members = new HashMap<InstanceId, URI>(  );
//        for ( int i = 0; i < instanceIds.length; i++ )
//        {
//            members.put( instanceIds[i], URI.create( initialHosts[i] ) );
//        }
//        ClusterConfiguration config = new ClusterConfiguration( "clusterName", initialHosts );
//        config.setMembers( members );
//
//        context = mock( ClusterContext.class );
//
//        when( context.getConfiguration() ).thenReturn( config );
//        when( context.getMyId() ).thenReturn( instanceIds[0] );
//
//        toTest = new HeartbeatContext(
//                context, mock( LearnerContext.class ),
//                Executors.newSingleThreadExecutor() );
//    }
//
//    @Test
//    public void testSaneInitialState()
//    {
//
//        // In config, not suspected yet
//        assertFalse( toTest.alive( instanceIds[0] ) );
//        // Not in config
//        assertFalse( toTest.alive( new InstanceId( 4 ) ) );
//
//        // By default, instances start off as alive
//        assertEquals( instanceIds.length, Iterables.count( toTest.getAlive() ) );
//        assertEquals( 0, toTest.getFailed().size() );
//
//        for ( InstanceId initialHost : instanceIds )
//        {
//            assertFalse( toTest.isFailed( initialHost ) );
//        }
//    }
//
//    @Test
//    public void testSuspicions()
//    {
//        InstanceId suspect = instanceIds[1];
//        toTest.suspect( suspect );
//        assertEquals( Collections.singleton( suspect ), toTest.getSuspicionsFor( context.getMyId() ) );
//        assertEquals( Collections.singletonList( context.getMyId() ), toTest.getSuspicionsOf( suspect ) );
//        // Being suspected by just one (us) is not enough
//        assertFalse( toTest.isFailed( suspect ) );
//        assertTrue( toTest.alive( suspect ) ); // This resets the suspicion above
//
//        // If we suspect an instance twice in a row, it shouldn't change its status in any way.
//        toTest.suspect( suspect );
//        toTest.suspect( suspect );
//        assertEquals( Collections.singleton( suspect ), toTest.getSuspicionsFor( context.getMyId() ) );
//        assertEquals( Collections.singletonList( context.getMyId() ), toTest.getSuspicionsOf( suspect ) );
//        assertFalse( toTest.isFailed( suspect ) );
//        assertTrue( toTest.alive( suspect ) );
//
//        // The other one sends suspicions too
//        InstanceId newSuspiciousBastard = instanceIds[2];
//        toTest.suspicions( newSuspiciousBastard, Collections.singleton( suspect ) );
//        toTest.suspect( suspect );
//        // Now two instances suspect it, it should be reported failed
//        assertEquals( Collections.singleton( suspect ), toTest.getSuspicionsFor( context.getMyId() ) );
//        assertEquals( Collections.singleton( suspect ), toTest.getSuspicionsFor( newSuspiciousBastard ) );
//        List<InstanceId> suspiciousBastards = new ArrayList<InstanceId>( 2 );
//        suspiciousBastards.add( context.getMyId() );
//        suspiciousBastards.add( newSuspiciousBastard );
//        assertEquals( suspiciousBastards, toTest.getSuspicionsOf( suspect ) );
//        assertTrue( toTest.isFailed( suspect ) );
//        assertTrue( toTest.alive( suspect ) );
//    }
//
//    @Test
//    public void testFailedInstanceBecomingAlive()
//    {
//        InstanceId suspect = instanceIds[1];
//        InstanceId newSuspiciousBastard = instanceIds[2];
//        toTest.suspicions( newSuspiciousBastard, Collections.singleton( suspect ) );
//        toTest.suspect( suspect );
//
//        // Just make sure
//        assertTrue( toTest.isFailed( suspect ) );
//
//        // Ok, here it is. We received a heartbeat, so it is alive.
//        toTest.alive( suspect );
//        // It must no longer be failed
//        assertFalse( toTest.isFailed( suspect ) );
//
//        // Simulate us stopping receiving heartbeats again
//        toTest.suspect( suspect );
//        assertTrue( toTest.isFailed( suspect ) );
//
//        // Assume the other guy started receiving heartbeats first
//        toTest.suspicions( newSuspiciousBastard, Collections.<InstanceId>emptySet() );
//        assertFalse( toTest.isFailed( suspect ) );
//    }
//
//    /**
//     * Tests the following scenario:
//     * Instance A (the one this test simulates) sees instance C down. B agrees.
//     * Instance A sees instance B down.
//     * Instance C starts responding again.
//     * Instance A should now consider C alive.
//     */
//    @Test
//    public void testOneInstanceComesAliveAfterAllOtherFail()
//    {
//        InstanceId instanceB = instanceIds[1];
//        InstanceId instanceC = instanceIds[2];
//
//        // Both A and B consider C down
//        toTest.suspect( instanceC );
//        toTest.suspicions( instanceB, Collections.singleton( instanceC ) );
//        assertTrue( toTest.isFailed( instanceC ) );
//
//        // A sees B as down
//        toTest.suspect( instanceB );
//        assertTrue( toTest.isFailed( instanceB ) );
//
//        // C starts responding again
//        assertTrue( toTest.alive( instanceC ) );
//
//        assertFalse( toTest.isFailed( instanceC ) );
//    }
}
