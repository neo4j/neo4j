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
package org.neo4j.kernel.impl.newapi;


import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ForbiddenLockInteractionException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.test.assertion.Assert;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class TransactionTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    void shouldRollbackWhenTxIsNotSuccess() throws Exception
    {
        // GIVEN
        long nodeId;
        int labelId;
        try ( Transaction tx = beginTransaction() )
        {
            // WHEN
            nodeId = tx.dataWrite().nodeCreate();
            labelId = tx.tokenWrite().labelGetOrCreateForName( "labello" );
            tx.dataWrite().nodeAddLabel( nodeId, labelId );

            // OBS: not marked as tx.success();
        }

        // THEN
        assertNoNode( nodeId );
    }

    @Test
    void shouldRollbackWhenTxIsFailed() throws Exception
    {
        // GIVEN
        long nodeId;
        int labelId;
        try ( Transaction tx = beginTransaction() )
        {
            // WHEN
            nodeId = tx.dataWrite().nodeCreate();
            labelId = tx.tokenWrite().labelGetOrCreateForName( "labello" );
            tx.dataWrite().nodeAddLabel( nodeId, labelId );

            tx.failure();
        }

        // THEN
        assertNoNode( nodeId );
    }

    @Test
    void shouldRollbackAndThrowWhenTxIsBothFailedAndSuccess() throws Exception
    {
        // GIVEN
        long nodeId;
        int labelId;

        Transaction tx = beginTransaction();
        nodeId = tx.dataWrite().nodeCreate();
        labelId = tx.tokenWrite().labelGetOrCreateForName( "labello" );
        tx.dataWrite().nodeAddLabel( nodeId, labelId );
        tx.failure();
        tx.success();

        // WHEN
        try
        {
            tx.close();
            fail( "Expected TransactionFailureException" );
        }
        catch ( TransactionFailureException e )
        {
            // wanted
        }

        // THEN
        assertNoNode( nodeId );
    }

    @Test
    void shouldForbidLockInteractions() throws Exception
    {
        // GIVEN
        int label;
        int propertyKey;

        try ( Transaction tx = beginTransaction() )
        {
            label = tx.tokenWrite().labelGetOrCreateForName( "Label" );
            propertyKey = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            tx.schemaWrite().indexCreate( SchemaDescriptor.forLabel( label, propertyKey ) );
            tx.success();
        }

        try ( Transaction tx = beginTransaction() )
        {
            assertAllowedLocks( label, propertyKey, tx );

            // WHEN
            tx.forbidLockInteractions();

            // THEN
            assertForbiddenLocks( label, propertyKey, tx );
        }
    }

    @Test
    void shouldAllowLockInteractions() throws Exception
    {
        // GIVEN
        int label;
        int propertyKey;

        try ( Transaction tx = beginTransaction() )
        {
            label = tx.tokenWrite().labelGetOrCreateForName( "Label" );
            propertyKey = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            tx.schemaWrite().indexCreate( SchemaDescriptor.forLabel( label, propertyKey ) );
            tx.success();
        }

        try ( Transaction tx = beginTransaction() )
        {
            // WHEN
            tx.forbidLockInteractions();
            tx.allowLockInteractions();

            // THEN
            assertAllowedLocks( label, propertyKey, tx );
        }
    }

    @Test
    void shouldForbidLockInteractionsIdempotently() throws Exception
    {
        // GIVEN
        int label;
        int propertyKey;

        try ( Transaction tx = beginTransaction() )
        {
            label = tx.tokenWrite().labelGetOrCreateForName( "Label" );
            propertyKey = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            tx.schemaWrite().indexCreate( SchemaDescriptor.forLabel( label, propertyKey ) );
            tx.success();
        }

        try ( Transaction tx = beginTransaction() )
        {
            // WHEN
            tx.forbidLockInteractions();
            tx.forbidLockInteractions();
            tx.forbidLockInteractions();
            tx.forbidLockInteractions();

            // THEN
            assertForbiddenLocks( label, propertyKey, tx );

            // WHEN
            tx.allowLockInteractions();
            tx.allowLockInteractions();
            tx.allowLockInteractions();
            tx.allowLockInteractions();

            // THEN
            assertAllowedLocks( label, propertyKey, tx );

            // WHEN
            tx.forbidLockInteractions();

            // THEN
            assertForbiddenLocks( label, propertyKey, tx );
        }
    }

    @Test
    void shouldCommitOnForbiddenLocks() throws Exception
    {
        // GIVEN
        long node;

        try ( Transaction tx = beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();

            // WHEN
            tx.forbidLockInteractions();
            tx.success();
        }

        // THEN
        assertNodeExists( node );
    }

    private void assertAllowedLocks( int label, int propertyKey, Transaction tx )
    {
        tx.schemaRead().index( label, propertyKey ); // acquires shared schema lock, but that's fine again
    }

    private void assertForbiddenLocks( int label, int propertyKey, Transaction tx )
    {
        Assert.assertException( () -> tx.schemaRead().index( label, propertyKey ), // acquires shared schema lock
                                ForbiddenLockInteractionException.class );
    }

    private void assertNoNode( long nodeId ) throws TransactionFailureException
    {
        try ( Transaction tx = beginTransaction();
                NodeCursor cursor = tx.cursors().allocateNodeCursor() )
        {
            tx.dataRead().singleNode( nodeId, cursor );
            assertFalse( cursor.next() );
        }
    }

    private void assertNodeExists( long nodeId ) throws TransactionFailureException
    {
        try ( Transaction tx = beginTransaction();
                NodeCursor cursor = tx.cursors().allocateNodeCursor() )
        {
            tx.dataRead().singleNode( nodeId, cursor );
            assertTrue( cursor.next() );
        }
    }
}
