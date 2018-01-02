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
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.Barrier;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.test.ha.ClusterRule;

import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;

public class IdBufferingRoleSwitchIT
{
    public @Rule ClusterRule clusterRule = new ClusterRule( getClass() )
             // Disable automatic sync so that the test can control this itself
            .withSharedSetting( HaSettings.pull_interval, "0" )
            .withSharedSetting( HaSettings.tx_push_factor, "0" )
            .withConsistencyCheckAfterwards();
    public @Rule OtherThreadRule<Void> t2 = new OtherThreadRule<>();

    @Test
    public void shouldNotSeeFreedIdsCrossRoleSwitch() throws Throwable
    {
        // GIVEN
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase firstMaster = cluster.getMaster();

        // WHEN
        // a node with a property
        Node node = createNodeWithProperties( firstMaster, 1 );
        // sync cluster
        cluster.sync();
        // a transaction on master which deletes the property
        deleteNode( node, firstMaster );
        triggerIdMaintenance( firstMaster );
        createNodeWithProperties( firstMaster, 1 ); // <-- this one reuses the same property id 0
        // a transaction T on slave which will be kept open using a barrier
        GraphDatabaseAPI slave = cluster.getAnySlave();
        Barrier.Control barrier = new Barrier.Control();
        Future<Void> t = t2.execute( barrierControlledReadTransaction( slave, barrier ) );
        // pull updates on slave
        barrier.await();
        slave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        // a role switch
        cluster.shutdown( firstMaster );
        cluster.await( masterAvailable( firstMaster ) );
        // close T
        barrier.release();
        t.get();
        triggerIdMaintenance( slave );

        // THEN the deleted property record should now not be in freelist on new master
        createNodeWithProperties( slave, 10 ); // <-- this transaction should introduce inconsistencies
        cluster.stop(); // <-- CC will be run here since that's configured above ^^^
    }

    private void triggerIdMaintenance( GraphDatabaseAPI db )
    {
        db.getDependencyResolver()
                .resolveDependency( NeoStoreDataSource.BufferedIdMaintenanceController.class )
                .maintenance();
    }

    private WorkerCommand<Void,Void> barrierControlledReadTransaction( final GraphDatabaseService slave,
            final Barrier.Control barrier )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = slave.beginTx() )
                {
                    barrier.reached();
                    tx.success();
                }
                catch ( Exception e )
                {
                    // This is OK, we expect this transaction to fail after role switch
                }
                finally
                {
                    barrier.release();
                }
                return null;
            }
        };
    }

    private void deleteNode( Node node, GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            node.delete();
            tx.success();
        }
    }

    private Node createNodeWithProperties( GraphDatabaseService db, int numberOfProperties )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            for ( int i = 0; i < numberOfProperties; i++ )
            {
                node.setProperty( "key" + i, "value" + i );
            }
            tx.success();
            return node;
        }
    }
}
