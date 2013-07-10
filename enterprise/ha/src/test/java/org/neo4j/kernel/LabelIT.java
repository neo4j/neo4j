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
package org.neo4j.kernel;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.SystemException;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

import static org.junit.Assert.assertEquals;
import static org.neo4j.cluster.ClusterSettings.default_timeout;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

public class LabelIT
{
    @Test
    public void creatingIndexOnMasterShouldHaveSlavesBuildItAsWell() throws Throwable
    {
        // GIVEN
        ClusterManager.ManagedCluster cluster = startCluster( clusterOfSize( 3 ) );
        org.neo4j.kernel.ha.HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        org.neo4j.kernel.ha.HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave(/*except*/slave1);

        Label label = label( "Person" );

        // WHEN
        javax.transaction.Transaction txOnSlave1 = createNodeAndKeepTxOpen( slave1, label );
        javax.transaction.Transaction txOnSlave2 = createNodeAndKeepTxOpen( slave2, label );

        commit(slave1, txOnSlave1);
        commit(slave2, txOnSlave2);

        // THEN
        assertEquals( getLabelId( slave1, label ), getLabelId( slave2, label ) );
    }

    private long getLabelId( HighlyAvailableGraphDatabase db, Label label ) throws LabelNotFoundKernelException
    {
        Transaction tx = db.beginTx();
        try
        {
            ThreadToStatementContextBridge bridge = db.getDependencyResolver().resolveDependency(
                    ThreadToStatementContextBridge.class );
            return bridge.getCtxForReading().keyReadOperations().labelGetForName( bridge.statementForReading(), label.name() );
        }
        finally
        {
            tx.finish();
        }
    }

    private void commit( HighlyAvailableGraphDatabase db, javax.transaction.Transaction tx ) throws Exception
    {
        TxManager txManager = db.getDependencyResolver().resolveDependency( TxManager.class );
        txManager.resume( tx );
        txManager.commit();
    }

    private javax.transaction.Transaction createNodeAndKeepTxOpen( HighlyAvailableGraphDatabase db, Label label )
            throws SystemException
    {
        db.beginTx();
        db.createNode( label );
        return db.getDependencyResolver().resolveDependency( TxManager.class ).suspend();
    }

    private final File storeDir = TargetDirectory.forTest( getClass() ).graphDbDir( true );
    private ClusterManager clusterManager;

    private ClusterManager.ManagedCluster startCluster( ClusterManager.Provider provider ) throws Throwable
    {
        return startCluster( provider, new HighlyAvailableGraphDatabaseFactory() );
    }

    private ClusterManager.ManagedCluster startCluster( ClusterManager.Provider provider,
                                                        HighlyAvailableGraphDatabaseFactory dbFactory ) throws Throwable
    {
        clusterManager = new ClusterManager( provider, storeDir, stringMap(
                default_timeout.name(), "1s", tx_push_factor.name(), "0" ),
                new HashMap<Integer, Map<String,String>>(), dbFactory );
        clusterManager.start();
        ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
        cluster.await( allSeesAllAsAvailable() );
        return cluster;
    }

    @After
    public void after() throws Throwable
    {
        if ( clusterManager != null )
            clusterManager.stop();
    }

}
