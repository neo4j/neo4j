/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.expirable.Expirable;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.Kernel;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.NodeItem;

import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;

public class KernelIT extends KernelIntegrationTest
{
    // TODO: Split this into area-specific tests, see PropertyIT.

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

        int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( "labello" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId );

        // 4: Close the StatementContext
        statement.close();

        // 5: Commit through the beans API
        transaction.success();
        transaction.close();
    }

    @Test
    public void mixingBeansApiWithKernelAPIForNestedTransaction() throws Exception
    {
        // GIVEN
        Transaction outerTx = db.beginTx();
        Statement statement = statementContextSupplier.get();

        // WHEN
        Node node = db.createNode();
        int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( "labello" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId );
        statement.close();
        outerTx.close();
    }

    @Test
    public void changesInTransactionContextShouldBeRolledBackWhenTxIsRolledBack() throws Exception
    {
        // GIVEN
        Node node;
        int labelId;
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = statementContextSupplier.get();

            // WHEN
            node = db.createNode();
            labelId = statement.tokenWriteOperations().labelGetOrCreateForName( "labello" );
            statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId );
            statement.close();
        }

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            try ( Statement statement = statementContextSupplier.get() )
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
            labelId = statement.tokenWriteOperations().labelGetOrCreateForName( "labello" );
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
        try ( Transaction tx = db.beginTx() )
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
        int labelId1 = statement.tokenWriteOperations().labelGetOrCreateForName( "labello1" );
        int labelId2 = statement.tokenWriteOperations().labelGetOrCreateForName( "labello2" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId1 );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId2 );
        statement.dataWriteOperations().nodeRemoveLabel( node.getId(), labelId2 );
        statement.close();
        tx.success();
        tx.close();

        // THEN
        tx = db.beginTx();
        statement = statementContextSupplier.get();
        assertEquals( PrimitiveIntCollections.asSet( new int[]{labelId1} ),
                PrimitiveIntCollections.asSet( statement.readOperations().nodeGetLabels( node.getId() ) ) );
        tx.close();
    }

    @Test
    public void transactionStateShouldReflectRemovingAddedLabelImmediately() throws Exception
    {
        Transaction tx = db.beginTx();
        Statement statement = statementContextSupplier.get();

        // WHEN
        Node node = db.createNode();
        int labelId1 = statement.tokenWriteOperations().labelGetOrCreateForName( "labello1" );
        int labelId2 = statement.tokenWriteOperations().labelGetOrCreateForName( "labello2" );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId1 );
        statement.dataWriteOperations().nodeAddLabel( node.getId(), labelId2 );
        statement.dataWriteOperations().nodeRemoveLabel( node.getId(), labelId2 );

        // THEN
        assertFalse( statement.readOperations().nodeHasLabel( node.getId(), labelId2 ) );
        assertEquals( PrimitiveIntCollections.asSet( new int[]{labelId1} ),
                PrimitiveIntCollections.asSet(  statement.readOperations().nodeGetLabels( node.getId() ) ) );

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
        int labelId1 = statement.tokenWriteOperations().labelGetOrCreateForName( "labello1" );
        int labelId2 = statement.tokenWriteOperations().labelGetOrCreateForName( "labello2" );
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
        PrimitiveIntSet labels = PrimitiveIntCollections.asSet(
                statement.readOperations().nodeGetLabels( node.getId() ) );
        assertFalse( statement.readOperations().nodeHasLabel( node.getId(), labelId2 ) );
        assertEquals( PrimitiveIntCollections.asSet( new int[]{labelId1} ), labels );
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
        int labelId1 = statement.tokenWriteOperations().labelGetOrCreateForName( "labello1" );
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
        Set<Integer> labels = PrimitiveIntCollections.toSet(
                statement.readOperations().nodeGetLabels( node.getId() ) );

        statement.close();
        tx.success();
        tx.close();

        assertThat( labels, equalTo( Collections.<Integer>emptySet() ) );
    }

    @Test
    public void addingNewLabelToNodeShouldRespondTrue() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        Statement statement = statementContextSupplier.get();
        int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( "mylabel" );
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
        int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( "mylabel" );
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
        int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( "mylabel" );
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
        int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( "mylabel" );
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

        Set<Long> nodes = PrimitiveLongCollections.toSet( statement.readOperations().nodesGetForLabel( labelId ) );

        statement.close();

        tx.success();
        tx.close();

        assertEquals( emptySet(), nodes );
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
        Set<Long> nodeSet = PrimitiveLongCollections.toSet( nodes );
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
        getOrCreateSchemaState( "my key", ExpirableContainer.of( "my state" ) );
        commit();

        // WHEN
        createIndex( statementInNewTransaction( SecurityContext.AUTH_DISABLED ) );
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
        IndexDescriptor idx = createIndex( statementInNewTransaction( SecurityContext.AUTH_DISABLED ) );
        commit();

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( db.schema().getIndexes().iterator().next(), 20, SECONDS );
            getOrCreateSchemaState( "my key", ExpirableContainer.of( "some state" ) );
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
        try ( KernelTransaction tx = kernel.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.read() ) )
        {
            ((Kernel)kernel).stop();
            tx.acquireStatement().readOperations().nodeExists( 0L );
            fail("Should have been terminated.");
        }
        catch ( TransactionTerminatedException e )
        {
            // Success
        }
    }

    @Test
    public void txReturnsCorrectIdWhenCommitted() throws Exception
    {
        executeDummyTxs( db, 42 );

        KernelTransaction tx = kernel.newTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
        try ( Statement statement = tx.acquireStatement() )
        {
            statement.dataWriteOperations().nodeCreate();
        }
        tx.success();

        long previousCommittedTxId = lastCommittedTxId( db );

        assertEquals( previousCommittedTxId + 1, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    @Test
    public void txReturnsCorrectIdWhenRolledBack() throws Exception
    {
        executeDummyTxs( db, 42 );

        KernelTransaction tx = kernel.newTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
        try ( Statement statement = tx.acquireStatement() )
        {
            statement.dataWriteOperations().nodeCreate();
        }
        tx.failure();

        assertEquals( KernelTransaction.ROLLBACK, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    @Test
    public void txReturnsCorrectIdWhenMarkedForTermination() throws Exception
    {
        executeDummyTxs( db, 42 );

        KernelTransaction tx = kernel.newTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
        try ( Statement statement = tx.acquireStatement() )
        {
            statement.dataWriteOperations().nodeCreate();
        }
        tx.markForTermination( Status.Transaction.Terminated );

        assertEquals( KernelTransaction.ROLLBACK, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    @Test
    public void txReturnsCorrectIdWhenFailedlAndMarkedForTermination() throws Exception
    {
        executeDummyTxs( db, 42 );

        KernelTransaction tx = kernel.newTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
        try ( Statement statement = tx.acquireStatement() )
        {
            statement.dataWriteOperations().nodeCreate();
        }
        tx.failure();
        tx.markForTermination( Status.Transaction.Terminated );

        assertEquals( KernelTransaction.ROLLBACK, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    @Test
    public void txReturnsCorrectIdWhenReadOnly() throws Exception
    {
        executeDummyTxs( db, 42 );

        KernelTransaction tx = kernel.newTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
        try ( Statement statement = tx.acquireStatement();
              Cursor<NodeItem> cursor = statement.readOperations().nodeCursorById( 1 ) )
        {
        }
        tx.success();

        assertEquals( KernelTransaction.READ_ONLY, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    private static void executeDummyTxs( GraphDatabaseService db, int count )
    {
        for ( int i = 0; i < count; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode();
                tx.success();
            }
        }
    }

    private static long lastCommittedTxId( GraphDatabaseAPI db )
    {
        TransactionIdStore txIdStore = db.getDependencyResolver().resolveDependency( TransactionIdStore.class );
        return txIdStore.getLastCommittedTransactionId();
    }

    private IndexDescriptor createIndex( Statement statement )
            throws SchemaKernelException, InvalidTransactionTypeKernelException
    {
        return statement.schemaWriteOperations().indexCreate( SchemaDescriptorFactory.forLabel(
                statement.tokenWriteOperations().labelGetOrCreateForName( "hello" ),
                statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "hepp" ) ) );
    }

    private <V extends Expirable> V getOrCreateSchemaState( String key, final V maybeSetThisState )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = statementContextSupplier.get();
            V state = statement.readOperations().schemaStateGetOrCreate( key, s -> maybeSetThisState );
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
            statement.readOperations().schemaStateGetOrCreate( key, s ->
            {
                result.set( false );
                return null;
            } );
            tx.success();
            return result.get();
        }
    }

    private static class ExpirableContainer implements Expirable
    {

        private String value;
        private boolean expired;

        static ExpirableContainer of( String value )
        {
            return new ExpirableContainer( value );
        }

        private ExpirableContainer( String value )
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }

        @Override
        public void expire()
        {
            expired = true;
        }

        @Override
        public boolean isExpired()
        {
            return expired;
        }
    }
}
