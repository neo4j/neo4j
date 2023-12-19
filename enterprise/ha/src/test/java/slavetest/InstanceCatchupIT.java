/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package slavetest;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientDatabaseFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;

/*
 * This test case ensures that instances with the same store id but very old txids
 * will successfully join with a full version of the store.
 */
public class InstanceCatchupIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule().withSharedSetting( keep_logical_logs, "1 txs" )
            .withSharedSetting( HaSettings.pull_interval, "0ms" );

    @Test
    public void slaveCanRecoverIfItFallsBackAfterJoining() throws Throwable
    {
        /*
         * This test tries to verify that if an instance, while being a cluster member, it falls behind then it can
         * recover by going through a store copy again. To test that we need 3 instances. A master, a slave and another
         * slave that will be the one that falls behind, named "victim". The cluster needs three instances so when we
         * disconnect the victim (so it can fall behind) the remaining two instances maintain quorum and can commit
         * transactions.
         * The order of operations is: Create cluster of 3, commit a transaction, disconnect victim, commit transactions,
         * prune all logs on master, reconnect victim. The new transactions should get to the victim.
         * This is an IT, not a unit test. The method of recovery is presumably a store copy, but the actual handling
         * happens through MasterClientResolver which implements the InvalidEpochHandler interface.
         */
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        HighlyAvailableGraphDatabase victim = cluster.getAnySlave( slave );
        String valueThatMakesItToVictim = "valueThatMakesItToVictim";
        createNode( victim, "key", valueThatMakesItToVictim );
        ClusterManager.RepairKit repairKit = cluster.fail( victim );

        /*
         * Ensure enough transaction logs exist so prunning can actually remove stuff
         */
        for ( int i = 0; i < 10; i++ )
        {
            createNode( master, "key", "valueTheVictimShouldNotSee" );
            checkPoint( master );
            rotateLog( master );
        }

        repairKit.repair();
        boolean success = false;
        long createdNode = -1;
        while ( !success )
        {
            try
            {
                createdNode = createNode( victim, "key", valueThatMakesItToVictim );
                success = true;
            }
            catch ( TransientTransactionFailureException | TransientDatabaseFailureException | IllegalStateException e )
            {
                /*
                 * All these exceptions are retryable and which one we end up getting depends on which part of the
                 * restart cycle the transaction begin or commit happened. We may get none, one or any combination of
                 * these during this loop. But eventually we should get out of here with a successful transaction.
                 */
            }
        }
        // If we got this far we're good, the slave that fell behind can execute txs which means it caught up.
        // What follows is a trivial sanity check to have some assertion in this test and basically to ensure that the
        // slave we think caught up is a member of the same cluster.
        String valueReadFromMaster;
        try ( Transaction tx = master.beginTx() )
        {
            valueReadFromMaster = (String) master.getNodeById( createdNode ).getProperty( "key" );
            tx.success();
        }
        assertEquals( valueThatMakesItToVictim, valueReadFromMaster );

    }

    private void rotateLog( HighlyAvailableGraphDatabase db ) throws IOException
    {
        db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
    }

    private void checkPoint( HighlyAvailableGraphDatabase db ) throws IOException
    {
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "test" )
        );
    }

    private long createNode( HighlyAvailableGraphDatabase db, String key, String value )
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( key, value );
            tx.success();
        }
        return node.getId();
    }
}
