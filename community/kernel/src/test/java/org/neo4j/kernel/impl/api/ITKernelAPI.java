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
package org.neo4j.kernel.impl.api;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.test.ImpermanentGraphDatabase;

public class ITKernelAPI
{
    /**
     * While we transition ownership from the Beans API to the Kernel API for core database
     * interactions, there will be a bit of a mess. Our first goal is an architecture like this:
     *
     *         Users
     *        /    \
     *  Beans API   Cypher
     *        \    /
     *      Kernel API
     *           |
     *  Kernel Implementation
     *
     * But our current intermediate architecture looks like this:
     *
     *           Users
     *        /        \
     *  Beans API <--- Cypher
     *     |    \    /
     *     |  Kernel API
     *     |      |
     *  Kernel Implementation
     *
     * Meaning Kernel API and Beans API both manipulate the underlying kernel, causing lots of corner cases.
     *
     * This test shows us how to use both the Kernel API and the Beans API together in the same transaction,
     * during the transition phase.
     */
    @Test
    public void mixingBeansApiWithKernelAPI() throws Exception
    {
        ThreadToStatementContextBridge statementContextProvider = db.getDependencyResolver().resolveDependency(
                ThreadToStatementContextBridge.class );

        // 1: Start your transactions through the Beans API
        Transaction beansAPITx = db.beginTx();

        // 2: Get a hold of a KernelAPI statement context for the *current* transaction this way:
        StatementContext statement = statementContextProvider.getCtxForWriting();

        // 3: Now you can interact through both the statement context and the kernel API to manipulate the
        //    same transaction.
        Node node = db.createNode();

        long labelId = statement.getOrCreateLabelId( "labello" );
        statement.addLabelToNode( labelId, node.getId() );

        // 4: Commit through the beans API
        beansAPITx.success();
        beansAPITx.finish();

        // You can use the kernel API on it's own, like this:
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        TransactionContext kernelAPITx = kernel.newTransactionContext();
        statement = kernelAPITx.newStatementContext();
        assertTrue( statement.isLabelSetOnNode( labelId, node.getId() ) );

        // NOTE: Transactions are still thread-bound right now in the Kernel API, meaning if you use
        // both the Kernel API to create transactions while a Beans API transaction is running in the same
        // thread, the results are undefined.

        // When the Kernel API implementation is done, the Kernel API transaction implementation is not meant
        // to be bound to threads.
    }

    private StatementContext getStatementContextFor( Transaction beansAPITx )
    {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    @Test
    public void mixingBeansApiWithKernelAPIForNestedTransaction() throws Exception
    {
        // GIVEN
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        Transaction outerTx = db.beginTx();
        TransactionContext tx = kernel.newTransactionContext();
        StatementContext statement = tx.newStatementContext();

        // WHEN
        Node node = db.createNode();
        long labelId = statement.getOrCreateLabelId( "labello" );
        statement.addLabelToNode( labelId, node.getId() );
        tx.success();
        tx.finish();
        outerTx.finish();

        // THEN
        tx = kernel.newTransactionContext();
        statement = tx.newStatementContext();
        assertFalse( statement.isLabelSetOnNode( labelId, node.getId() ) );
    }
    
    @Test
    public void shouldBeAbleToRollBackTransactionContext() throws Exception
    {
        // GIVEN
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        TransactionContext tx = kernel.newTransactionContext();
        StatementContext statement = tx.newStatementContext();

        // WHEN
        Node node = db.createNode();
        long labelId = statement.getOrCreateLabelId( "labello" );
        statement.addLabelToNode( labelId, node.getId() );
        tx.finish();

        // THEN
        tx = kernel.newTransactionContext();
        statement = tx.newStatementContext();
        assertFalse( statement.isLabelSetOnNode( labelId, node.getId() ) );
    }
    
    @Test
    public void shouldNotBeAbleToCommitIfFailedTransactionContext() throws Exception
    {
        // GIVEN
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        TransactionContext tx = kernel.newTransactionContext();
        StatementContext statement = tx.newStatementContext();

        // WHEN
        Node node = db.createNode();
        long labelId = statement.getOrCreateLabelId( "labello" );
        statement.addLabelToNode( labelId, node.getId() );
        tx.failure();
        tx.success();
        tx.finish();

        // THEN
        tx = kernel.newTransactionContext();
        statement = tx.newStatementContext();
        assertFalse( statement.isLabelSetOnNode( labelId, node.getId() ) );
    }
    
    private GraphDatabaseAPI db;
    
    @Before
    public void before() throws Exception
    {
        db = new ImpermanentGraphDatabase();
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }
}
