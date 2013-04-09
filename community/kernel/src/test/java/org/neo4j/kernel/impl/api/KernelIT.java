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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.test.ImpermanentGraphDatabase;

public class KernelIT
{
    /**
     * While we transition ownership from the Beans API to the Kernel API for core database
     * interactions, there will be a bit of a mess. Our first goal is an architecture like this:
     * <p/>
     * Users
     * /    \
     * Beans API   Cypher
     * \    /
     * Kernel API
     * |
     * Kernel Implementation
     * <p/>
     * But our current intermediate architecture looks like this:
     * <p/>
     * Users
     * /        \
     * Beans API <--- Cypher
     * |    \    /
     * |  Kernel API
     * |      |
     * Kernel Implementation
     * <p/>
     * Meaning Kernel API and Beans API both manipulate the underlying kernel, causing lots of corner cases. Most
     * notably, those corner cases are related to Transactions, and the interplay between three transaction APIs:
     * - The Beans API
     * - The JTA Transaction Manager API
     * - The Kernel TransactionContext API
     * <p/>
     * In the long term, the goal is for JTA compliant stuff to live outside of the kernel, as an addon. The Kernel
     * API will rule supreme over the land of transactions. We are a long way away from there, however, so as a first
     * intermediary step, the JTA transaction manager rules supreme, and the Kernel API piggybacks on it.
     * <p/>
     * This test shows us how to use both the Kernel API and the Beans API together in the same transaction,
     * during the transition phase.
     */
    @Test
    public void mixingBeansApiWithKernelAPI() throws Exception
    {
        // 1: Start your transactions through the Beans API
        Transaction beansAPITx = db.beginTx();

        // 2: Get a hold of a KernelAPI statement context for the *current* transaction this way:
        StatementContext statement = statementContextProvider.getCtxForWriting();

        // 3: Now you can interact through both the statement context and the kernel API to manipulate the
        //    same transaction.
        Node node = db.createNode();

        long labelId = statement.getOrCreateLabelId( "labello" );
        statement.addLabelToNode( labelId, node.getId() );

        // 4: Close the StatementContext
        statement.close();

        // 5: Commit through the beans API
        beansAPITx.success();
        beansAPITx.finish();

        // NOTE: Transactions are still thread-bound right now, because we use JTA to "own" transactions,
        // meaning if you use
        // both the Kernel API to create transactions while a Beans API transaction is running in the same
        // thread, the results are undefined.

        // When the Kernel API implementation is done, the Kernel API transaction implementation is not meant
        // to be bound to threads.
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
        statement.close();
        tx.commit();
        outerTx.finish();
    }

    @Test
    public void changesInTransactionContextShouldBeRolledBackWhenTxIsRolledBack() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        StatementContext statement = statementContextProvider.getCtxForWriting();

        // WHEN
        Node node = db.createNode();
        long labelId = statement.getOrCreateLabelId( "labello" );
        statement.addLabelToNode( labelId, node.getId() );
        statement.close();
        tx.finish();

        // THEN
        statement = statementContextProvider.getCtxForReading();
        assertFalse( statement.isLabelSetOnNode( labelId, node.getId() ) );
    }

    @Test
    public void shouldNotBeAbleToCommitIfFailedTransactionContext() throws Exception
    {
        Transaction tx = db.beginTx();
        StatementContext statement = statementContextProvider.getCtxForWriting();

        // WHEN
        Node node = db.createNode();
        long labelId = statement.getOrCreateLabelId( "labello" );
        statement.addLabelToNode( labelId, node.getId() );
        statement.close();
        tx.failure();
        tx.success();
        tx.finish();

        // THEN
        statement = statementContextProvider.getCtxForReading();
        assertFalse( statement.isLabelSetOnNode( labelId, node.getId() ) );
    }

    @Test
    public void transactionStateShouldRemovePreviouslyAddedLabel() throws Exception
    {
        Transaction tx = db.beginTx();
        StatementContext statement = statementContextProvider.getCtxForWriting();

        // WHEN
        Node node = db.createNode();
        long labelId1 = statement.getOrCreateLabelId( "labello1" );
        long labelId2 = statement.getOrCreateLabelId( "labello2" );
        statement.addLabelToNode( labelId1, node.getId() );
        statement.addLabelToNode( labelId2, node.getId() );
        statement.removeLabelFromNode( labelId2, node.getId() );
        statement.close();
        tx.success();
        tx.finish();

        // THEN
        statement = statementContextProvider.getCtxForReading();
        assertEquals( asSet( labelId1 ), asSet( statement.getLabelsForNode( node.getId() ) ) );
    }

    @Test
    public void transactionStateShouldReflectRemovingAddedLabelImmediately() throws Exception
    {
        Transaction tx = db.beginTx();
        StatementContext statement = statementContextProvider.getCtxForWriting();

        // WHEN
        Node node = db.createNode();
        long labelId1 = statement.getOrCreateLabelId( "labello1" );
        long labelId2 = statement.getOrCreateLabelId( "labello2" );
        statement.addLabelToNode( labelId1, node.getId() );
        statement.addLabelToNode( labelId2, node.getId() );
        statement.removeLabelFromNode( labelId2, node.getId() );

        // THEN
        assertFalse( statement.isLabelSetOnNode( labelId2, node.getId() ) );
        assertEquals( asSet( labelId1 ), asSet( statement.getLabelsForNode( node.getId() ) ) );

        statement.close();
        tx.success();
        tx.finish();
    }

    @Test
    public void transactionStateShouldReflectRemovingLabelImmediately() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        StatementContext statement = statementContextProvider.getCtxForWriting();
        Node node = db.createNode();
        long labelId1 = statement.getOrCreateLabelId( "labello1" );
        long labelId2 = statement.getOrCreateLabelId( "labello2" );
        statement.addLabelToNode( labelId1, node.getId() );
        statement.addLabelToNode( labelId2, node.getId() );
        statement.close();
        tx.success();
        tx.finish();
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();

        // WHEN
        statement.removeLabelFromNode( labelId2, node.getId() );

        // THEN
        Iterator<Long> labelsIterator = statement.getLabelsForNode( node.getId() );
        Set<Long> labels = asSet( labelsIterator );
        assertFalse( statement.isLabelSetOnNode( labelId2, node.getId() ) );
        assertEquals( asSet( labelId1 ), labels );
        statement.close();
        tx.success();
        tx.finish();
    }

    @Test
    public void addingNewLabelToNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        StatementContext statement = statementContextProvider.getCtxForWriting();
        long labelId = statement.getOrCreateLabelId( "mylabel" );
        statement.addLabelToNode( labelId, node.getId() );
        statement.close();
        tx.success();
        tx.finish();

        // WHEN
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        boolean added = statement.addLabelToNode( labelId, node.getId() );

        // THEN
        assertFalse( "Shouldn't have been added now", added );
    }

    @Test
    public void addingExistingLabelToNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        StatementContext statement = statementContextProvider.getCtxForWriting();
        long labelId = statement.getOrCreateLabelId( "mylabel" );
        statement.close();
        tx.success();
        tx.finish();

        // WHEN
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        boolean added = statement.addLabelToNode( labelId, node.getId() );

        // THEN
        assertTrue( "Should have been added now", added );
    }

    @Test
    public void removingExistingLabelFromNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        StatementContext statement = statementContextProvider.getCtxForWriting();
        long labelId = statement.getOrCreateLabelId( "mylabel" );
        statement.addLabelToNode( labelId, node.getId() );
        statement.close();
        tx.success();
        tx.finish();

        // WHEN
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        boolean removed = statement.removeLabelFromNode( labelId, node.getId() );

        // THEN
        assertTrue( "Should have been removed now", removed );
    }

    @Test
    public void removingNonExistentLabelFromNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        StatementContext statement = statementContextProvider.getCtxForWriting();
        long labelId = statement.getOrCreateLabelId( "mylabel" );
        statement.close();
        tx.success();
        tx.finish();

        // WHEN
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        boolean removed = statement.removeLabelFromNode( labelId, node.getId() );

        // THEN
        assertFalse( "Shouldn't have been removed now", removed );
    }

    @Test
    public void deletingNodeWithLabelsShouldHaveThoseLabelRemovalsReflectedInTransaction() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Label label = label( "labello" );
        Node node = db.createNode( label );
        tx.success();
        tx.finish();

        tx = db.beginTx();
        StatementContext statement = statementContextProvider.getCtxForWriting();

        // WHEN
        statement.deleteNode( node.getId() );

        // Then
        long labelId = statement.getLabelId( label.name() );
        Set<Long> labels = asSet( statement.getLabelsForNode( node.getId() ) );
        boolean labelIsSet = statement.isLabelSetOnNode( labelId, node.getId() );
        Set<Long> nodes = asSet( statement.getNodesWithLabel( labelId ) );

        statement.close();

        tx.success();
        tx.finish();

        assertEquals( asSet(), nodes );
        assertEquals( asSet(), labels );
        assertFalse( "Label should not be set on node here", labelIsSet );
    }

    @Test
    public void deletingNodeWithLabelsShouldHaveRemovalReflectedInLabelScans() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Label label = label( "labello" );
        Node node = db.createNode( label );
        tx.success();
        tx.finish();

        // AND GIVEN I DELETE IT
        tx = db.beginTx();
        node.delete();
        tx.success();
        tx.finish();

        // WHEN
        tx = db.beginTx();
        StatementContext statement = statementContextProvider.getCtxForWriting();
        long labelId = statement.getLabelId( label.name() );
        Iterator<Long> nodes = statement.getNodesWithLabel( labelId );
        Set<Long> nodeSet = asSet( nodes );
        tx.success();
        tx.finish();

        // THEN
        assertThat( nodeSet, equalTo( Collections.<Long>emptySet() ) );
    }

    private GraphDatabaseAPI db;
    private ThreadToStatementContextBridge statementContextProvider;

    @Before
    public void before() throws Exception
    {
        db = new ImpermanentGraphDatabase();
        statementContextProvider = db.getDependencyResolver().resolveDependency(
                ThreadToStatementContextBridge.class );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }
}
