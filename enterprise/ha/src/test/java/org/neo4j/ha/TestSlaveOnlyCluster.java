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
package org.neo4j.ha;

import static org.neo4j.test.TargetDirectory.forTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.test.TargetDirectory;

@Ignore("There are no slave only instances yet")
public class TestSlaveOnlyCluster
{
    private HighlyAvailableGraphDatabase master;
    private final HighlyAvailableGraphDatabase[] slaves = new HighlyAvailableGraphDatabase[2];
    private final TargetDirectory dir = forTest( getClass() );

    @Before
    public void doBefore() throws Exception
    {
        /*
         * instantiate master and slaves
         */
    }

    @After
    public void doAfter() throws Exception
    {
        for ( HighlyAvailableGraphDatabase db : slaves )
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
        master.shutdown();
    }

    @Test
    public void testMasterElectionAfterMasterRecoversInSlaveOnlyCluster() throws Exception
    {
        /*
         * Shutdown and start master. Since the other two instances are slaves they will not elect
         * themselves. So when the old master comes back up it should be picked up as the master.
         */
        master.shutdown();
        Thread.sleep( 1000 ); // Make sure everything is shut down, including ZK threads
        /*
         * Instantiate master here
         */
        while ( !master.isMaster() )
        {
            ;
        }
        while ( !slaves[0].getInstanceState().equals( HighAvailabilityMemberState.SLAVE ) )
        {
            ;
        }
        while ( !slaves[1].getInstanceState().equals( HighAvailabilityMemberState.SLAVE ) )
        {
            ;
        }
        // Execute a tx on one slave, make sure a master has been picked
        Transaction tx = slaves[0].beginTx();
        slaves[0].createNode();
        tx.success();
        tx.finish();
    }
}
