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
import org.neo4j.internal.kernel.api.exceptions.FrozenLocksException;
import org.neo4j.internal.kernel.api.exceptions.LocksNotFrozenException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class TransactionTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    void shouldRollbackWhenTxIsNotSuccess() throws Exception
    {
        // GIVEN
        long nodeId;
        int labelId;
        try ( KernelTransaction tx = beginTransaction() )
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
        try ( KernelTransaction tx = beginTransaction() )
        {
            // WHEN
            nodeId = tx.dataWrite().nodeCreate();
            labelId = tx.tokenWrite().labelGetOrCreateForName( "labello" );
            tx.dataWrite().nodeAddLabel( nodeId, labelId );

            tx.rollback();
        }

        // THEN
        assertNoNode( nodeId );
    }

    @Test
    void shouldFreezeLockInteractions() throws Exception
    {
        // GIVEN
        int label;
        int propertyKey;
        LabelSchemaDescriptor schema;

        try ( KernelTransaction tx = beginTransaction() )
        {
            label = tx.tokenWrite().labelGetOrCreateForName( "Label" );
            propertyKey = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            schema = SchemaDescriptor.forLabel( label, propertyKey );
            tx.schemaWrite().indexCreate( schema, "my index" );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            assertAllowedLocks( tx, schema );

            // WHEN
            tx.freezeLocks();

            // THEN
            assertFrozenLocks( tx, schema );
        }
    }

    @Test
    void shouldThawLockInteractions() throws Exception
    {
        // GIVEN
        int label;
        int propertyKey;
        LabelSchemaDescriptor schema;

        try ( KernelTransaction tx = beginTransaction() )
        {
            label = tx.tokenWrite().labelGetOrCreateForName( "Label" );
            propertyKey = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            schema = SchemaDescriptor.forLabel( label, propertyKey );
            tx.schemaWrite().indexCreate( schema, "my index" );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            // WHEN
            tx.freezeLocks();
            tx.thawLocks();

            // THEN
            assertAllowedLocks( tx, schema );
        }
    }

    @Test
    void shouldThrowOnThawOfNotFrozenLocks() throws Exception
    {
        // GIVEN
        int label;
        int propertyKey;
        LabelSchemaDescriptor schema;

        try ( KernelTransaction tx = beginTransaction() )
        {
            label = tx.tokenWrite().labelGetOrCreateForName( "Label" );
            propertyKey = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            schema = SchemaDescriptor.forLabel( label, propertyKey );
            tx.schemaWrite().indexCreate( schema, "my index" );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            // WHEN
            assertThrows( LocksNotFrozenException.class, tx::thawLocks );

            // THEN
            assertAllowedLocks( tx, schema );
        }
    }

    @Test
    void shouldNestFreezeLocks() throws Exception
    {
        // GIVEN
        int label;
        int propertyKey;
        LabelSchemaDescriptor schema;

        try ( KernelTransaction tx = beginTransaction() )
        {
            label = tx.tokenWrite().labelGetOrCreateForName( "Label" );
            propertyKey = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            schema = SchemaDescriptor.forLabel( label, propertyKey );
            tx.schemaWrite().indexCreate( schema, "my index" );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            // WHEN
            tx.freezeLocks();
            tx.freezeLocks();
            tx.freezeLocks();
            tx.freezeLocks();

            // THEN
            assertFrozenLocks( tx, schema );

            // WHEN
            tx.thawLocks();
            assertFrozenLocks( tx, schema );
            tx.thawLocks();
            assertFrozenLocks( tx, schema );
            tx.thawLocks();
            assertFrozenLocks( tx, schema );
            tx.thawLocks();

            // THEN
            assertAllowedLocks( tx, schema );

            // WHEN
            tx.freezeLocks();

            // THEN
            assertFrozenLocks( tx, schema );
        }
    }

    @Test
    void shouldCommitOnFrozenLocks() throws Exception
    {
        // GIVEN
        long node;

        try ( KernelTransaction tx = beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();

            // WHEN
            tx.freezeLocks();
            tx.commit();
        }

        // THEN
        assertNodeExists( node );
    }

    // HELPERS

    private void assertAllowedLocks( KernelTransaction tx, SchemaDescriptor schema )
    {
        tx.schemaRead().index( schema ).forEachRemaining( index -> {} ); // acquires shared schema lock, but that's fine again
    }

    private void assertFrozenLocks( KernelTransaction tx, SchemaDescriptor schema )
    {
        assertThrows( FrozenLocksException.class, () -> tx.schemaRead().index( schema ).forEachRemaining( index -> { } ) );
    }

    private void assertNoNode( long nodeId ) throws TransactionFailureException
    {
        try ( KernelTransaction tx = beginTransaction();
                NodeCursor cursor = tx.cursors().allocateNodeCursor() )
        {
            tx.dataRead().singleNode( nodeId, cursor );
            assertFalse( cursor.next() );
        }
    }

    private void assertNodeExists( long nodeId ) throws TransactionFailureException
    {
        try ( KernelTransaction tx = beginTransaction();
                NodeCursor cursor = tx.cursors().allocateNodeCursor() )
        {
            tx.dataRead().singleNode( nodeId, cursor );
            assertTrue( cursor.next() );
        }
    }
}
