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
package org.neo4j.kernel.impl.api.integrationtest;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;

public class KernelIT extends KernelIntegrationTest
{
    // TODO: Split this into area-specific tests, see PropertyIT.

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

        long labelId = statement.labelGetOrCreateForName( "labello" );
        statement.nodeAddLabel( node.getId(), labelId );

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
        long labelId = statement.labelGetOrCreateForName( "labello" );
        statement.nodeAddLabel( node.getId(), labelId );
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
        long labelId = statement.labelGetOrCreateForName( "labello" );
        statement.nodeAddLabel( node.getId(), labelId );
        statement.close();
        tx.finish();

        // THEN
        statement = statementContextProvider.getCtxForReading();
        try
        {
            statement.nodeHasLabel( node.getId(), labelId );
            fail( "should have thrown exception" );
        }
        catch ( EntityNotFoundException e )
        {
            // Yay!
        }
    }

    @Test
    public void shouldNotBeAbleToCommitIfFailedTransactionContext() throws Exception
    {
        Transaction tx = db.beginTx();
        StatementContext statement = statementContextProvider.getCtxForWriting();

        // WHEN
        Node node = db.createNode();
        long labelId = statement.labelGetOrCreateForName( "labello" );
        statement.nodeAddLabel( node.getId(), labelId );
        statement.close();
        tx.failure();
        tx.success();
        tx.finish();

        // THEN
        statement = statementContextProvider.getCtxForReading();
        try
        {
            statement.nodeHasLabel( node.getId(), labelId );
            fail( "should have thrown exception" );
        }
        catch ( EntityNotFoundException e )
        {
            // Yay!
        }
    }

    @Test
    public void transactionStateShouldRemovePreviouslyAddedLabel() throws Exception
    {
        Transaction tx = db.beginTx();
        StatementContext statement = statementContextProvider.getCtxForWriting();

        // WHEN
        Node node = db.createNode();
        long labelId1 = statement.labelGetOrCreateForName( "labello1" );
        long labelId2 = statement.labelGetOrCreateForName( "labello2" );
        statement.nodeAddLabel( node.getId(), labelId1 );
        statement.nodeAddLabel( node.getId(), labelId2 );
        statement.nodeRemoveLabel( node.getId(), labelId2 );
        statement.close();
        tx.success();
        tx.finish();

        // THEN
        statement = statementContextProvider.getCtxForReading();
        assertEquals( asSet( labelId1 ), asSet( statement.nodeGetLabels( node.getId() ) ) );
    }

    @Test
    public void transactionStateShouldReflectRemovingAddedLabelImmediately() throws Exception
    {
        Transaction tx = db.beginTx();
        StatementContext statement = statementContextProvider.getCtxForWriting();

        // WHEN
        Node node = db.createNode();
        long labelId1 = statement.labelGetOrCreateForName( "labello1" );
        long labelId2 = statement.labelGetOrCreateForName( "labello2" );
        statement.nodeAddLabel( node.getId(), labelId1 );
        statement.nodeAddLabel( node.getId(), labelId2 );
        statement.nodeRemoveLabel( node.getId(), labelId2 );

        // THEN
        assertFalse( statement.nodeHasLabel( node.getId(), labelId2 ) );
        assertEquals( asSet( labelId1 ), asSet( statement.nodeGetLabels( node.getId() ) ) );

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
        long labelId1 = statement.labelGetOrCreateForName( "labello1" );
        long labelId2 = statement.labelGetOrCreateForName( "labello2" );
        statement.nodeAddLabel( node.getId(), labelId1 );
        statement.nodeAddLabel( node.getId(), labelId2 );
        statement.close();
        tx.success();
        tx.finish();
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();

        // WHEN
        statement.nodeRemoveLabel( node.getId(), labelId2 );

        // THEN
        Iterator<Long> labelsIterator = statement.nodeGetLabels( node.getId() );
        Set<Long> labels = asSet( labelsIterator );
        assertFalse( statement.nodeHasLabel( node.getId(), labelId2 ) );
        assertEquals( asSet( labelId1 ), labels );
        statement.close();
        tx.success();
        tx.finish();
    }

    @Test
    public void labelShouldBeRemovedAfterCommit() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        StatementContext statement = statementContextProvider.getCtxForWriting();
        Node node = db.createNode();
        long labelId1 = statement.labelGetOrCreateForName( "labello1" );
        statement.nodeAddLabel( node.getId(), labelId1 );
        statement.close();
        tx.success();
        tx.finish();

        // WHEN
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        statement.nodeRemoveLabel( node.getId(), labelId1 );
        statement.close();
        tx.success();
        tx.finish();

        // THEN
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        Iterator<Long> labels = statement.nodeGetLabels( node.getId() );
        statement.close();
        tx.success();
        tx.finish();

        assertThat( asSet( labels ), equalTo( Collections.<Long>emptySet() ) );
    }

    @Test
    public void addingNewLabelToNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        StatementContext statement = statementContextProvider.getCtxForWriting();
        long labelId = statement.labelGetOrCreateForName( "mylabel" );
        statement.nodeAddLabel( node.getId(), labelId );
        statement.close();
        tx.success();
        tx.finish();

        // WHEN
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        boolean added = statement.nodeAddLabel( node.getId(), labelId );

        // THEN
        assertFalse( "Shouldn't have been added now", added );
        tx.finish();
    }

    @Test
    public void addingExistingLabelToNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        StatementContext statement = statementContextProvider.getCtxForWriting();
        long labelId = statement.labelGetOrCreateForName( "mylabel" );
        statement.close();
        tx.success();
        tx.finish();

        // WHEN
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        boolean added = statement.nodeAddLabel( node.getId(), labelId );

        // THEN
        assertTrue( "Should have been added now", added );
        tx.finish();
    }

    @Test
    public void removingExistingLabelFromNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        StatementContext statement = statementContextProvider.getCtxForWriting();
        long labelId = statement.labelGetOrCreateForName( "mylabel" );
        statement.nodeAddLabel( node.getId(), labelId );
        statement.close();
        tx.success();
        tx.finish();

        // WHEN
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        boolean removed = statement.nodeRemoveLabel( node.getId(), labelId );

        // THEN
        assertTrue( "Should have been removed now", removed );
        tx.finish();
    }

    @Test
    public void removingNonExistentLabelFromNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        StatementContext statement = statementContextProvider.getCtxForWriting();
        long labelId = statement.labelGetOrCreateForName( "mylabel" );
        statement.close();
        tx.success();
        tx.finish();

        // WHEN
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        boolean removed = statement.nodeRemoveLabel( node.getId(), labelId );

        // THEN
        assertFalse( "Shouldn't have been removed now", removed );
        tx.finish();
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
        statement.nodeDelete( node.getId() );

        // Then
        long labelId = statement.labelGetForName( label.name() );
        Set<Long> labels = asSet( statement.nodeGetLabels( node.getId() ) );
        boolean labelIsSet = statement.nodeHasLabel( node.getId(), labelId );
        Set<Long> nodes = asSet( statement.nodesGetForLabel( labelId ) );

        statement.close();

        tx.success();
        tx.finish();

        assertEquals( emptySetOf( Long.class ), nodes );
        assertEquals( emptySetOf( Long.class ), labels );
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
        long labelId = statement.labelGetForName( label.name() );
        Iterator<Long> nodes = statement.nodesGetForLabel( labelId );
        Set<Long> nodeSet = asSet( nodes );
        tx.success();
        tx.finish();

        // THEN
        assertThat( nodeSet, equalTo( Collections.<Long>emptySet() ) );
    }

    @Test
    public void schemaStateShouldBeEvictedOnIndexComingOnline() throws Exception
    {
        // GIVEN
        newTransaction();
        getOrCreateSchemaState( "my key", "my state" );
        commit();

        // WHEN
        newTransaction();
        createIndex( );
        commit();

        newTransaction();
        db.schema().awaitIndexOnline( db.schema().getIndexes().iterator().next(), 20, SECONDS );
        commit();

        // THEN
        assertFalse( schemaStateContains("my key") );
    }

    @Test
    public void schemaStateShouldBeEvictedOnIndexDropped() throws Exception
    {
        // GIVEN
        newTransaction();
        IndexDescriptor idx = createIndex( );
        commit();

        newTransaction();
        db.schema().awaitIndexOnline( db.schema().getIndexes().iterator().next(), 20, SECONDS );
        getOrCreateSchemaState( "my key", "some state" );
        commit();

        // WHEN
        newTransaction();
        statement.indexDrop( idx );
        commit();

        // THEN
        assertFalse( schemaStateContains("my key") );
    }

    private IndexDescriptor createIndex( ) throws SchemaKernelException
    {
        return statement.indexCreate( statement.labelGetOrCreateForName( "hello" ),
                                      statement.propertyKeyGetOrCreateForName( "hepp" ) );
    }

    private String getOrCreateSchemaState( String key, final String maybeSetThisState )
    {
        Transaction tx;StatementContext statement;
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        String state = statement.schemaStateGetOrCreate( key, new Function<String, String>()
        {
            @Override
            public String apply( String s )
            {
                return maybeSetThisState;
            }
        } );
        tx.success();
        tx.finish();
        return state;
    }

    private boolean schemaStateContains( String key )
    {
        Transaction tx;StatementContext statement;
        tx = db.beginTx();
        statement = statementContextProvider.getCtxForWriting();
        boolean state = statement.schemaStateContains( key );
        tx.success();
        tx.finish();
        return state;
    }
}
