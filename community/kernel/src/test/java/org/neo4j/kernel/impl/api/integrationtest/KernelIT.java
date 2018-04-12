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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class KernelIT extends KernelIntegrationTest
{

    @Test
    public void mixingBeansApiWithKernelAPI() throws Exception
    {
        // 1: Start your transactions through the Beans API
        Transaction transaction = db.beginTx();

        // 2: Get a hold of a KernelAPI transaction this way:
        KernelTransaction ktx = statementContextSupplier.getKernelTransactionBoundToThisThread( true );

        // 3: Now you can interact through both the statement context and the kernel API to manipulate the
        //    same transaction.
        Node node = db.createNode();

        int labelId = ktx.tokenWrite().labelGetOrCreateForName( "labello" );
        ktx.dataWrite().nodeAddLabel( node.getId(), labelId );

        // 5: Commit through the beans API
        transaction.success();
        transaction.close();
    }

    @Test
    public void addingExistingLabelToNodeShouldRespondFalse() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        int labelId;
        KernelTransaction ktx = statementContextSupplier.getKernelTransactionBoundToThisThread( true );
        labelId = ktx.tokenWrite().labelGetOrCreateForName( "mylabel" );

        tx.success();
        tx.close();

        // WHEN
        tx = db.beginTx();
        ktx = statementContextSupplier.getKernelTransactionBoundToThisThread( true );

        boolean added = ktx.dataWrite().nodeAddLabel( node.getId(), labelId );

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
        KernelTransaction ktx = statementContextSupplier.getKernelTransactionBoundToThisThread( true );

        int labelId = ktx.tokenWrite().labelGetOrCreateForName( "mylabel" );
        ktx.dataWrite().nodeAddLabel( node.getId(), labelId );

        tx.success();
        tx.close();

        // WHEN
        tx = db.beginTx();
        ktx = statementContextSupplier.getKernelTransactionBoundToThisThread( true );
        boolean removed = ktx.dataWrite().nodeRemoveLabel( node.getId(), labelId );

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
        int labelId;
        KernelTransaction ktx = statementContextSupplier.getKernelTransactionBoundToThisThread( true );
        labelId = ktx.tokenWrite().labelGetOrCreateForName( "mylabel" );
        tx.success();
        tx.close();

        // WHEN
        tx = db.beginTx();
        ktx = statementContextSupplier.getKernelTransactionBoundToThisThread( true );
        boolean removed = ktx.dataWrite().nodeRemoveLabel( node.getId(), labelId );

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
        KernelTransaction ktx = statementContextSupplier.getKernelTransactionBoundToThisThread( true );

        // WHEN
        ktx.dataWrite().nodeDelete( node.getId() );

        // Then
        int labelId = ktx.tokenRead().nodeLabel( label.name() );
        try ( NodeCursor nodeCursor = ktx.cursors().allocateNodeCursor() )
        {
            ktx.dataRead().singleNode( node.getId(), nodeCursor );
            assertFalse( nodeCursor.next() );
        }

        try ( NodeLabelIndexCursor nodeCursor = ktx.cursors().allocateNodeLabelIndexCursor() )
        {
            ktx.dataRead().nodeLabelScan( labelId, nodeCursor );
            assertFalse( nodeCursor.next() );
        }

        ktx.close();
        tx.success();
        tx.close();
    }

    @Test
    public void deletingNodeWithLabelsShouldHaveRemovalReflectedInLabelScans()
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
        Set<Long> nodeSet = new HashSet<>();
        KernelTransaction ktx =
                statementContextSupplier.getKernelTransactionBoundToThisThread( true );
        try ( NodeLabelIndexCursor nodes = ktx.cursors().allocateNodeLabelIndexCursor() )
        {
            int labelId = ktx.tokenRead().nodeLabel( label.name() );
            ktx.dataRead().nodeLabelScan( labelId, nodes );
            while ( nodes.next() )
            {
                nodeSet.add( nodes.nodeReference() );
            }
        }
        tx.success();
        tx.close();

        // THEN
        assertThat( nodeSet, equalTo( Collections.<Long>emptySet() ) );
    }

    @Test
    public void schemaStateShouldBeEvictedOnIndexComingOnline() throws Exception
    {
        // GIVEN
        schemaWriteInNewTransaction();
        getOrCreateSchemaState( "my key", "my state" );
        commit();

        // WHEN
        createIndex( newTransaction( AUTH_DISABLED ) );
        commit();

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 20, SECONDS );
            tx.success();
        }
        // THEN schema state is eventually updated (clearing the schema cache is not atomic with respect to flipping
        // the new index to the ONLINE state, but happens as soon as possible *after* the index becomes ONLINE).
        assertEventually( "Schema state should have been updated",
                () -> schemaStateContains( "my key" ), is( false ), 1, TimeUnit.SECONDS );
    }

    @Test
    public void schemaStateShouldBeEvictedOnIndexDropped() throws Exception
    {
        // GIVEN
        IndexReference idx = createIndex( newTransaction( AUTH_DISABLED ) );
        commit();

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 20, SECONDS );
            getOrCreateSchemaState( "my key", "some state" );
            tx.success();
        }
        // WHEN
        schemaWriteInNewTransaction().indexDrop( idx );
        commit();

        // THEN schema state should be immediately updated (this works because the schema cache is updated during
        // transaction apply, while the schema lock is held).
        assertFalse( schemaStateContains( "my key" ) );
    }

    @Test
    public void txReturnsCorrectIdWhenCommitted() throws Exception
    {
        executeDummyTxs( db, 42 );

        org.neo4j.internal.kernel.api.Transaction tx = newTransaction( AUTH_DISABLED );
        tx.dataWrite().nodeCreate();
        tx.success();

        long previousCommittedTxId = lastCommittedTxId( db );

        assertEquals( previousCommittedTxId + 1, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    @Test
    public void txReturnsCorrectIdWhenRolledBack() throws Exception
    {
        executeDummyTxs( db, 42 );

        org.neo4j.internal.kernel.api.Transaction tx = newTransaction( AUTH_DISABLED );
        tx.dataWrite().nodeCreate();
        tx.failure();

        assertEquals( KernelTransaction.ROLLBACK, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    @Test
    public void txReturnsCorrectIdWhenMarkedForTermination() throws Exception
    {
        executeDummyTxs( db, 42 );

        org.neo4j.internal.kernel.api.Transaction tx = newTransaction( AUTH_DISABLED );
        tx.dataWrite().nodeCreate();
        tx.markForTermination( Status.Transaction.Terminated );

        assertEquals( KernelTransaction.ROLLBACK, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    @Test
    public void txReturnsCorrectIdWhenFailedlAndMarkedForTermination() throws Exception
    {
        executeDummyTxs( db, 42 );

        org.neo4j.internal.kernel.api.Transaction tx = newTransaction( AUTH_DISABLED );
        tx.dataWrite().nodeCreate();
        tx.failure();
        tx.markForTermination( Status.Transaction.Terminated );

        assertEquals( KernelTransaction.ROLLBACK, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    @Test
    public void txReturnsCorrectIdWhenReadOnly() throws Exception
    {
        executeDummyTxs( db, 42 );

        org.neo4j.internal.kernel.api.Transaction tx = newTransaction();
        try ( NodeCursor node = tx.cursors().allocateNodeCursor() )
        {
            tx.dataRead().singleNode( 1, node );
            node.next();
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

    private IndexReference createIndex( org.neo4j.internal.kernel.api.Transaction transaction )
            throws SchemaKernelException, InvalidTransactionTypeKernelException
    {
        TokenWrite tokenWrite = transaction.tokenWrite();
        SchemaWrite schemaWrite = transaction.schemaWrite();
        LabelSchemaDescriptor schemaDescriptor = forLabel( tokenWrite.labelGetOrCreateForName( "hello" ),
                tokenWrite.propertyKeyGetOrCreateForName( "hepp" ) );
        return schemaWrite.indexCreate( schemaDescriptor );
    }

    private String getOrCreateSchemaState( String key, final String maybeSetThisState )
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx =
                    statementContextSupplier.getKernelTransactionBoundToThisThread( true );
            String state = ktx.schemaRead().schemaStateGetOrCreate( key, s -> maybeSetThisState );
            tx.success();
            return state;
        }
    }

    private boolean schemaStateContains( String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = statementContextSupplier.getKernelTransactionBoundToThisThread( true );
            final AtomicBoolean result = new AtomicBoolean( true );
            ktx.schemaRead().schemaStateGetOrCreate( key, s ->
            {
                result.set( false );
                return null;
            } );
            tx.success();
            return result.get();
        }
    }
}
