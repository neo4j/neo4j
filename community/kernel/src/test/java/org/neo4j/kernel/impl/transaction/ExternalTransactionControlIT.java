/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.OtherThreadExecutor;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.Neo4jMatchers.hasLabel;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;

public class ExternalTransactionControlIT
{

    public @Rule
    ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private enum Labels implements Label
    {
        MY_LABEL;
    }

    @Test
    public void shouldAllowSuspendingAndResumingTransactions() throws Exception
    {
        // Given
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        TransactionManager tm = db.getDependencyResolver().resolveDependency( TransactionManager.class );

        Node node = createNode();

        // And that I have added a label to a node in a transaction
        db.beginTx();
        node.addLabel( Labels.MY_LABEL );

        // When
        Transaction jtaTx = tm.suspend();

        // Then
        assertThat(node, inTx(db, not( hasLabel( Labels.MY_LABEL ) )));

        // And when
        tm.resume( jtaTx );
        // Then
        assertTrue("The label should be visible when I've resumed the transaction.", node.hasLabel( Labels.MY_LABEL ));
    }

    @Test
    public void shouldBeAbleToUseJTATransactionManagerForTxManagement() throws Exception
    {
        // Given
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        TransactionManager tm = db.getDependencyResolver().resolveDependency( TransactionManager.class );

        // When
        tm.begin();
        Node node = db.createNode();
        node.addLabel( Labels.MY_LABEL );
        tm.commit();

        // Then
        assertThat(node, inTx(db, hasLabel( Labels.MY_LABEL )));
    }

    @Test
    public void shouldBeAbleToMoveTransactionToAnotherThread() throws Exception
    {
        // Given
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        final TransactionManager tm = db.getDependencyResolver().resolveDependency( TransactionManager.class );

        final Node node = createNode();

        // And that I have added a label to a node in a transaction
        db.beginTx();
        node.addLabel( Labels.MY_LABEL );

        // And that I suspend the transaction in this thread
        final Transaction jtaTx = tm.suspend();

        // When
        OtherThreadExecutor<Boolean> otherThread = new OtherThreadExecutor<Boolean>( "Thread to resume tx in", null );
        boolean result = otherThread.execute( new OtherThreadExecutor.WorkerCommand<Boolean, Boolean>()
        {
            @Override
            public Boolean doWork( Boolean ignore )
            {
                try
                {
                    tm.resume( jtaTx );
                    // Then
                    return node.hasLabel( Labels.MY_LABEL );
                }
                catch ( Throwable e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );

        // Then
        assertTrue("The label should be visible when I've resumed the transaction.", result);
    }

    private Node createNode()
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        org.neo4j.graphdb.Transaction tx = db.beginTx();
        Node node = db.createNode();
        tx.success();
        tx.finish();
        return node;
    }
}
