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

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.test.assertion.Assert.assertEventually;

class KernelIT extends KernelIntegrationTest
{
    @Test
    void mixingBeansApiWithKernelAPI() throws Exception
    {
        // 1: Start your transactions through the Beans API
        Transaction transaction = db.beginTx();

        // 2: Get a hold of a KernelAPI transaction this way:
        KernelTransaction ktx = ((InternalTransaction) transaction).kernelTransaction();

        // 3: Now you can interact through both the statement context and the kernel API to manipulate the
        //    same transaction.
        Node node = transaction.createNode();

        int labelId = ktx.tokenWrite().labelGetOrCreateForName( "labello" );
        ktx.dataWrite().nodeAddLabel( node.getId(), labelId );

        // 4: Commit through the beans API
        transaction.commit();
    }

    @Test
    void schemaStateShouldBeEvictedOnIndexComingOnline() throws Exception
    {
        // GIVEN
        try ( Transaction tx = db.beginTx() )
        {
            getOrCreateSchemaState( tx, "my key", "my state" );
            tx.commit();
        }

        // WHEN
        createIndex( newTransaction( AUTH_DISABLED ) );
        commit();

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 20, SECONDS );
            tx.commit();
        }
        // THEN schema state is eventually updated (clearing the schema cache is not atomic with respect to flipping
        // the new index to the ONLINE state, but happens as soon as possible *after* the index becomes ONLINE).
        assertEventually( "Schema state should have been updated",
                () -> schemaStateContains( "my key" ), is( false ), 1, TimeUnit.SECONDS );
    }

    @Test
    void schemaStateShouldBeEvictedOnIndexDropped() throws Exception
    {
        // GIVEN
        IndexDescriptor idx = createIndex( newTransaction( AUTH_DISABLED ) );
        commit();

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 20, SECONDS );
            getOrCreateSchemaState( tx, "my key", "some state" );
            tx.commit();
        }
        // WHEN
        schemaWriteInNewTransaction().indexDrop( idx );
        commit();

        // THEN schema state should be immediately updated (this works because the schema cache is updated during
        // transaction apply, while the schema lock is held).
        assertFalse( schemaStateContains( "my key" ) );
    }

    @Test
    void txReturnsCorrectIdWhenCommitted() throws Exception
    {
        executeDummyTxs( db, 42 );

        KernelTransaction tx = newTransaction( AUTH_DISABLED );
        tx.dataWrite().nodeCreate();

        long previousCommittedTxId = lastCommittedTxId( db );

        assertEquals( previousCommittedTxId + 1, tx.commit() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void txReturnsCorrectIdWhenRolledBack() throws Exception
    {
        executeDummyTxs( db, 42 );

        KernelTransaction tx = newTransaction( AUTH_DISABLED );
        tx.dataWrite().nodeCreate();

        assertEquals( KernelTransaction.ROLLBACK_ID, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void txReturnsCorrectIdWhenMarkedForTermination() throws Exception
    {
        executeDummyTxs( db, 42 );

        KernelTransaction tx = newTransaction( AUTH_DISABLED );
        tx.dataWrite().nodeCreate();
        tx.markForTermination( Status.Transaction.Terminated );

        assertEquals( KernelTransaction.ROLLBACK_ID, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void txReturnsCorrectIdWhenFailedAndMarkedForTermination() throws Exception
    {
        executeDummyTxs( db, 42 );

        KernelTransaction tx = newTransaction( AUTH_DISABLED );
        tx.dataWrite().nodeCreate();
        tx.markForTermination( Status.Transaction.Terminated );

        assertEquals( KernelTransaction.ROLLBACK_ID, tx.closeTransaction() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void txReturnsCorrectIdWhenReadOnly() throws Exception
    {
        executeDummyTxs( db, 42 );

        KernelTransaction tx = newTransaction();
        try ( NodeCursor node = tx.cursors().allocateNodeCursor() )
        {
            tx.dataRead().singleNode( 1, node );
            node.next();
        }

        assertEquals( KernelTransaction.READ_ONLY_ID, tx.commit() );
        assertFalse( tx.isOpen() );
    }

    private static void executeDummyTxs( GraphDatabaseService db, int count )
    {
        for ( int i = 0; i < count; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.createNode();
                tx.commit();
            }
        }
    }

    private static long lastCommittedTxId( GraphDatabaseAPI db )
    {
        TransactionIdStore txIdStore = db.getDependencyResolver().resolveDependency( TransactionIdStore.class );
        return txIdStore.getLastCommittedTransactionId();
    }

    private static IndexDescriptor createIndex( KernelTransaction transaction )
            throws KernelException
    {
        TokenWrite tokenWrite = transaction.tokenWrite();
        SchemaWrite schemaWrite = transaction.schemaWrite();
        LabelSchemaDescriptor schema = forLabel( tokenWrite.labelGetOrCreateForName( "hello" ),
                tokenWrite.propertyKeyGetOrCreateForName( "hepp" ) );
        return schemaWrite.indexCreate( schema, null );
    }

    private void getOrCreateSchemaState( Transaction tx, String key, final String maybeSetThisState )
    {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        ktx.schemaRead().schemaStateGetOrCreate( key, s -> maybeSetThisState );
    }

    private boolean schemaStateContains( String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            final AtomicBoolean result = new AtomicBoolean( true );
            ktx.schemaRead().schemaStateGetOrCreate( key, s ->
            {
                result.set( false );
                return null;
            } );
            tx.commit();
            return result.get();
        }
    }
}
