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
package org.neo4j.kernel;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;

public class LabelIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule();

    protected ClusterManager.ManagedCluster cluster;

    @Before
    public void setup()
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void creatingIndexOnMasterShouldHaveSlavesBuildItAsWell()
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

    private static long getLabelId( HighlyAvailableGraphDatabase db, Label label )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            ThreadToStatementContextBridge bridge = threadToStatementContextBridgeFrom( db );
            return bridge.getKernelTransactionBoundToThisThread( true ).tokenRead().nodeLabel( label.name() );
        }
    }

    private static void commit( TransactionContinuation txc )
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
        private KernelTransaction graphDbTx;
        private final ThreadToStatementContextBridge bridge;

        private TransactionContinuation( HighlyAvailableGraphDatabase db )
        {
            this.db = db;
            this.bridge = threadToStatementContextBridgeFrom( db );
        }

        public void begin()
        {
            db.beginTx();
            graphDbTx = bridge.getKernelTransactionBoundToThisThread( false );
        }

        public void suspend()
        {
            graphDbTx = bridge.getKernelTransactionBoundToThisThread( true );
            bridge.unbindTransactionFromCurrentThread();
        }

        public void resume()
        {
            bridge.bindTransactionToCurrentThread( graphDbTx );
        }

        public void commit()
        {
            try
            {
                graphDbTx.close();
            }
            catch ( TransactionFailureException e )
            {
                throw new org.neo4j.graphdb.TransactionFailureException( e.getMessage(), e );
            }
        }
    }
}
