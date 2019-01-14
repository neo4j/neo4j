/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeCursor;
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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

        // 4: Commit through the beans API
        transaction.success();
        transaction.close();
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
        return schemaWrite.indexCreate( schemaDescriptor, null );
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
