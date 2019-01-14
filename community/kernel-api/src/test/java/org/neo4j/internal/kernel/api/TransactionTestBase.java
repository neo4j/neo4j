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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public abstract class TransactionTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    public void shouldRollbackWhenTxIsNotSuccess() throws Exception
    {
        // GIVEN
        long nodeId;
        int labelId;
        try ( Transaction tx = session.beginTransaction() )
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
    public void shouldRollbackWhenTxIsFailed() throws Exception
    {
        // GIVEN
        long nodeId;
        int labelId;
        try ( Transaction tx = session.beginTransaction() )
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
    public void shouldRollbackAndThrowWhenTxIsBothFailedAndSuccess() throws Exception
    {
        // GIVEN
        long nodeId;
        int labelId;

        Transaction tx = session.beginTransaction();
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

    private void assertNoNode( long nodeId ) throws TransactionFailureException
    {
        try ( Transaction tx = session.beginTransaction();
                NodeCursor cursor = tx.cursors().allocateNodeCursor() )
        {
            tx.dataRead().singleNode( nodeId, cursor );
            assertFalse( cursor.next() );
        }
    }
}
