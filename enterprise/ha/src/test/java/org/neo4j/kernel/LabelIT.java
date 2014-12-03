/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.AbstractClusterTest;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

public class LabelIT extends AbstractClusterTest
{
    public LabelIT()
    {
        super( clusterOfSize( 3 ) );
    }

    @Test
    public void creatingIndexOnMasterShouldHaveSlavesBuildItAsWell() throws Throwable
    {
        // GIVEN
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave(/*except*/slave1 );

        Label label = label( "Person" );

        // WHEN
        TransactionContinuation txOnSlave1 = createNodeAndKeepTxOpen( slave1, label );
        TransactionContinuation txOnSlave2 = createNodeAndKeepTxOpen( slave2, label );

        commit( txOnSlave1 );
        commit( txOnSlave2 );

        // THEN
        assertEquals( getLabelId( slave1, label ), getLabelId( slave2, label ) );
    }

    @Override
    protected void configureClusterMember( GraphDatabaseBuilder builder, String clusterName, InstanceId serverId )
    {
        builder.setConfig( ClusterSettings.default_timeout, "1s" );
        builder.setConfig( HaSettings.tx_push_factor, "0" );
    }

    private static long getLabelId( HighlyAvailableGraphDatabase db, Label label )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            ThreadToStatementContextBridge bridge = threadToStatementContextBridgeFrom( db );
            return bridge.instance().readOperations().labelGetForName( label.name() );
        }
    }

    private static void commit( TransactionContinuation txc ) throws Exception
    {
        txc.resume();
        txc.commit();
    }

    private static TransactionContinuation createNodeAndKeepTxOpen( HighlyAvailableGraphDatabase db, Label label )
    {
        TransactionContinuation txc = new TransactionContinuation( db );
        txc.begin();
        db.createNode( label );
        txc.suspend();
        return txc;
    }

    private static ThreadToStatementContextBridge threadToStatementContextBridgeFrom( HighlyAvailableGraphDatabase db )
    {
        return db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }

    private static class TransactionContinuation
    {
        private final HighlyAvailableGraphDatabase db;
        private TopLevelTransaction graphDbTx;
        private final ThreadToStatementContextBridge bridge;

        private TransactionContinuation( HighlyAvailableGraphDatabase db )
        {
            this.db = db;
            this.bridge = threadToStatementContextBridgeFrom( db );
        }

        public void begin()
        {
            graphDbTx = (TopLevelTransaction) db.beginTx();
        }

        public void suspend()
        {
            graphDbTx = bridge.getTopLevelTransactionBoundToThisThread( true );
            bridge.unbindTransactionFromCurrentThread();
        }

        public void resume() throws Exception
        {
            bridge.bindTransactionToCurrentThread( graphDbTx );
        }

        public void commit()
        {
            graphDbTx.close();
        }
    }
}
