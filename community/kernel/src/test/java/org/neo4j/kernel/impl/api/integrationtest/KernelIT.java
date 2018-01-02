/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Function;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.Kernel;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;

public class KernelIT extends KernelIntegrationTest
{
    // TODO: Split this into area-specific tests, see PropertyIT.

    /**
     * While we transition ownership from the Beans API to the Kernel API for core database
     * interactions, there will be a bit of a mess. Our first goal is an architecture like this:
     * <p>
     * Users
     * /    \
     * Beans API   Cypher
     * \    /
     * Kernel API
     * |
     * Kernel Implementation
     * <p>
     * But our current intermediate architecture looks like this:
     * <p>
     * Users
     * /        \
     * Beans API <--- Cypher
     * |    \    /
     * |  Kernel API
     * |      |
     * Kernel Implementation
     * <p>
     * Meaning Kernel API and Beans API both manipulate the underlying kernel, causing lots of corner cases. Most
     * notably, those corner cases are related to Transactions, and the interplay between three transaction APIs:
     * - The Beans API
     * - The JTA Transaction Manager API
     * - The Kernel TransactionContext API
     * <p>
     * In the long term, the goal is for JTA compliant stuff to live outside of the kernel, as an addon. The Kernel
     * API will rule supreme over the land of transactions. We are a long way away from there, however, so as a first
     * intermediary step, the JTA transaction manager rules supreme, and the Kernel API piggybacks on it.
     * <p>
     * This test shows us how to use both the Kernel API and the Beans API together in the same transaction,
     * during the transition phase.
     */
    @Test
    public void mixingBeansApiWithKernelAPI() throws Exception
    {
        // 1: Start your transactions through the Beans API
        Transaction transaction = db.beginTx();

        // 2: Get a hold of a KernelAPI statement context for the *current* transaction this way:
        Statement statement = statementContextSupplier.get();

        // 3: Now you can interact through both the statement context and the kernel API to manipulate the
        //    same transaction.
        Node node = db.createNode();

        int labelId = statement.dataWriteOperations().labelGetOrCreateForName( "labello" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId );

        // 4: Close the StatementContext
        statement.close();

        // 5: Commit through the beans API
        transaction.success();
        transaction.close();


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
        Transaction outerTx = db.beginTx();
        Statement statement = statementContextSupplier.get();

        // WHEN
        Node node = db.createNode();
        int labelId = statement.dataWriteOperations().labelGetOrCreateForName( "labello" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId );
        statement.close();
        outerTx.close();
    }

    @Test
    public void changesInTransactionContextShouldBeRolledBackWhenTxIsRolledBack() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Statement statement = statementContextSupplier.get();

        // WHEN
        Node node = db.createNode();
        int labelId = statement.dataWriteOperations().labelGetOrCreateForName( "labello" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId );
        statement.close();
        tx.close();

        // THEN
        tx = db.beginTx();
        statement = statementContextSupplier.get();
        try
        {
            statement.readOperations().nodeHasLabel( node.getId(), labelId );
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
        // WHEN
        Node node = null;
        int labelId = -1;
        TransactionFailureException expectedException = null;
        try ( Transaction transaction = db.beginTx() )
        {
            Statement statement = statementContextSupplier.get();
            node = db.createNode();
            labelId = statement.dataWriteOperations().labelGetOrCreateForName( "labello" );
            statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId );
            statement.close();
            transaction.failure();
            transaction.success();
        }
        catch ( TransactionFailureException e )
        {
            expectedException = e;
        }
        finally
        {
            Assert.assertNotNull( "Should have failed", expectedException );
        }

        // THEN
        try (Transaction tx = db.beginTx())
        {
            Statement statement = statementContextSupplier.get();
            try
            {
                statement.readOperations().nodeHasLabel( node.getId(), labelId );
                fail( "should have thrown exception" );
            }
            catch ( EntityNotFoundException e )
            {
                // Yay!
            }
        }
    }

    @Test
    public void transactionStateShouldRemovePreviouslyAddedLabel() throws Exception
    {
        Transaction tx = db.beginTx();

        Statement statement = statementContextSupplier.get();

        // WHEN
        Node node = db.createNode();
        int labelId1 = statement.dataWriteOperations().labelGetOrCreateForName( "labello1" );
        int labelId2 = statement.dataWriteOperations().labelGetOrCreateForName( "labello2" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId1 );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId2 );
        statement.dataWriteOperations().nodeRemoveLabel( node.getId(), labelId2 );
        statement.close();
        tx.success();
        tx.close();

        // THEN
        tx = db.beginTx();
        statement = statementContextSupplier.get();
        assertEquals( asSet( labelId1 ), asSet( statement.readOperations().nodeGetLabels( node.getId() ) ) );
        tx.close();

    }

    @Test
    public void transactionStateShouldReflectRemovingAddedLabelImmediately() throws Exception
    {
        Transaction tx = db.beginTx();
        Statement statement = statementContextSupplier.get();

        // WHEN
        Node node = db.createNode();
        int labelId1 = statement.dataWriteOperations().labelGetOrCreateForName( "labello1" );
        int labelId2 = statement.dataWriteOperations().labelGetOrCreateForName( "labello2" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId1 );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId2 );
        statement.dataWriteOperations().nodeRemoveLabel( node.getId(), labelId2 );

        // THEN
        assertFalse( statement.readOperations().nodeHasLabel( node.getId(), labelId2 ) );
        assertEquals( asSet( labelId1 ), asSet( statement.readOperations().nodeGetLabels( node.getId() ) ) );

        statement.close();
        tx.success();
        tx.close();
    }

    @Test
    public void transactionStateShouldReflectRemovingLabelImmediately() throws Exception
    {
        // GIVEN

        Transaction tx = db.beginTx();
        Statement statement = statementContextSupplier.get();
        Node node = db.createNode();
        int labelId1 = statement.dataWriteOperations().labelGetOrCreateForName( "labello1" );
        int labelId2 = statement.dataWriteOperations().labelGetOrCreateForName( "labello2" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId1 );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId2 );
        statement.close();
        tx.success();
        tx.close();

        tx = db.beginTx();
        statement = statementContextSupplier.get();

        // WHEN
        statement.dataWriteOperations().nodeRemoveLabel( node.getId(), labelId2 );

        // THEN
        PrimitiveIntIterator labelsIterator = statement.readOperations().nodeGetLabels( node.getId() );
        Set<Integer> labels = asSet( labelsIterator );
        assertFalse( statement.readOperations().nodeHasLabel( node.getId(), labelId2 ) );
        assertEquals( asSet( labelId1 ), labels );
        statement.close();
        tx.success();
        tx.close();
    }

    @Test
    public void labelShouldBeRemovedAfterCommit() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Statement statement = statementContextSupplier.get();
        Node node = db.createNode();
        int labelId1 = statement.dataWriteOperations().labelGetOrCreateForName( "labello1" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId1 );
        statement.close();
        tx.success();
        tx.close();


        // WHEN
        tx = db.beginTx();
        statement = statementContextSupplier.get();
        statement.dataWriteOperations().nodeRemoveLabel( node.getId(), labelId1 );
        statement.close();
        tx.success();
        tx.close();

        // THEN
        tx = db.beginTx();
        statement = statementContextSupplier.get();
        PrimitiveIntIterator labels = statement.readOperations().nodeGetLabels( node.getId() );

        statement.close();
        tx.success();
        tx.close();

        assertThat( asSet( labels ), equalTo( Collections.<Integer>emptySet() ) );
    }

    @Test
    public void addingNewLabelToNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        Statement statement = statementContextSupplier.get();
        int labelId = statement.dataWriteOperations().labelGetOrCreateForName( "mylabel" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId );
        statement.close();
        tx.success();
        tx.close();

        // WHEN
        tx = db.beginTx();
        statement = statementContextSupplier.get();
        boolean added = statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId );
        tx.close();

        // THEN
        assertFalse( "Shouldn't have been added now", added );
    }

    @Test
    public void addingExistingLabelToNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        Statement statement = statementContextSupplier.get();
        int labelId = statement.dataWriteOperations().labelGetOrCreateForName( "mylabel" );
        statement.close();
        tx.success();
        tx.close();

        // WHEN
        tx = db.beginTx();
        statement = statementContextSupplier.get();
        boolean added = statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId );
        tx.close();

        // THEN
        assertTrue( "Should have been added now", added );
    }

    @Test
    public void removingExistingLabelFromNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        Statement statement = statementContextSupplier.get();
        int labelId = statement.dataWriteOperations().labelGetOrCreateForName( "mylabel" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId );
        statement.close();
        tx.success();
        tx.close();

        // WHEN
        tx = db.beginTx();
        statement = statementContextSupplier.get();
        boolean removed = statement.dataWriteOperations().nodeRemoveLabel( node.getId(), labelId );

        // THEN
        assertTrue( "Should have been removed now", removed );
        tx.close();
    }

    @Test
    public void removingNonExistentLabelFromNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        Statement statement = statementContextSupplier.get();
        int labelId = statement.dataWriteOperations().labelGetOrCreateForName( "mylabel" );
        statement.close();
        tx.success();
        tx.close();

        // WHEN
        tx = db.beginTx();
        statement = statementContextSupplier.get();
        boolean removed = statement.dataWriteOperations().nodeRemoveLabel( node.getId(), labelId );

        // THEN
        assertFalse( "Shouldn't have been removed now", removed );
        tx.close();
    }

    @Test
    public void deletingNodeWithLabelsShouldHaveThoseLabelRemovalsReflectedInTransaction() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Label label = label( "labello" );
        Node node = db.createNode( label );
        tx.success();
        tx.close();

        tx = db.beginTx();
        Statement statement = statementContextSupplier.get();

        // WHEN
        statement.dataWriteOperations().nodeDelete( node.getId() );

        // Then
        int labelId = statement.readOperations().labelGetForName( label.name() );
        try
        {
            statement.readOperations().nodeGetLabels( node.getId() );
            fail();
        }
        catch ( EntityNotFoundException e )
        {
            // Ok
        }

        try
        {
            statement.readOperations().nodeHasLabel( node.getId(), labelId );
            fail();
        }
        catch ( EntityNotFoundException e )
        {
            // Ok
        }

        Set<Long> nodes = asSet( statement.readOperations().nodesGetForLabel( labelId ) );

        statement.close();

        tx.success();
        tx.close();

        assertEquals( emptySetOf( Long.class ), nodes );
    }

    @Test
    public void deletingNodeWithLabelsShouldHaveRemovalReflectedInLabelScans() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Label label = label( "labello" );
        Node node = db.createNode( label );
        tx.success();
        tx.close();

        // AND GIVEN I DELETE IT
        tx = db.beginTx();
        node.delete();
        tx.success();
        tx.close();

        // WHEN
        tx = db.beginTx();
        Statement statement = statementContextSupplier.get();
        int labelId = statement.readOperations().labelGetForName( label.name() );
        PrimitiveLongIterator nodes = statement.readOperations().nodesGetForLabel( labelId );
        Set<Long> nodeSet = asSet( nodes );
        tx.success();
        tx.close();

        // THEN
        assertThat( nodeSet, equalTo( Collections.<Long>emptySet() ) );
    }

    @Test
    public void schemaStateShouldBeEvictedOnIndexComingOnline() throws Exception
    {
        // GIVEN
        schemaWriteOperationsInNewTransaction();
        getOrCreateSchemaState( "my key", "my state" );
        commit();

        // WHEN
        createIndex( schemaWriteOperationsInNewTransaction() );
        commit();

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( db.schema().getIndexes().iterator().next(), 20, SECONDS );
            tx.success();
        }
        // THEN
        assertFalse( schemaStateContains( "my key" ) );
    }

    @Test
    public void schemaStateShouldBeEvictedOnIndexDropped() throws Exception
    {
        // GIVEN
        IndexDescriptor idx = createIndex( schemaWriteOperationsInNewTransaction() );
        commit();

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( db.schema().getIndexes().iterator().next(), 20, SECONDS );
            getOrCreateSchemaState( "my key", "some state" );
            tx.success();
        }
        // WHEN
        schemaWriteOperationsInNewTransaction().indexDrop( idx );
        commit();

        // THEN
        assertFalse( schemaStateContains("my key") );
    }

    @Test
    public void shouldKillTransactionsOnShutdown() throws Throwable
    {
        // Given
        assumeThat(kernel, instanceOf( Kernel.class ));

        // Then
        try ( KernelTransaction tx = kernel.newTransaction() )
        {
            ((Kernel)kernel).stop();
            tx.acquireStatement().readOperations().nodeExists( 0l );
            fail("Should have been terminated.");
        }
        catch( TransactionTerminatedException e )
        {
            // Success
        }
    }

    private IndexDescriptor createIndex( SchemaWriteOperations schemaWriteOperations ) throws SchemaKernelException
    {
        return schemaWriteOperations.indexCreate( schemaWriteOperations.labelGetOrCreateForName( "hello" ),
                schemaWriteOperations.propertyKeyGetOrCreateForName( "hepp" ) );
    }

    private String getOrCreateSchemaState( String key, final String maybeSetThisState )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = statementContextSupplier.get();
            String state = statement.readOperations().schemaStateGetOrCreate( key, new Function<String,String>()
            {
                @Override
                public String apply( String s )
                {
                    return maybeSetThisState;
                }
            } );
            tx.success();
            return state;
        }
    }

    private boolean schemaStateContains( String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = statementContextSupplier.get();
            final AtomicBoolean result = new AtomicBoolean( true );
            statement.readOperations().schemaStateGetOrCreate( key, new Function<String,Object>()
            {
                @Override
                public Object apply( String s )
                {
                    result.set( false );
                    return null;
                }
            } );
            tx.success();
            return result.get();
        }
    }
}
