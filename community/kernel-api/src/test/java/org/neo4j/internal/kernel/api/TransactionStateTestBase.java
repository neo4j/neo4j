/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class TransactionStateTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    public void shouldDetectNodeDeletedInTransaction() throws Exception
    {
        // GIVEN
        long deletedInTx, unaffected, addedInTx, addedAndRemovedInTx;
        try ( Transaction tx = beginTransaction() )
        {
            deletedInTx = tx.dataWrite().nodeCreate();
            unaffected = tx.dataWrite().nodeCreate();
            tx.success();
        }

        try ( Transaction tx = beginTransaction() )
        {
            // WHEN
            addedInTx = tx.dataWrite().nodeCreate();
            addedAndRemovedInTx = tx.dataWrite().nodeCreate();
            tx.dataWrite().nodeDelete( deletedInTx );
            tx.dataWrite().nodeDelete( addedAndRemovedInTx );

            // THEN
            assertFalse( tx.dataRead().nodeDeletedInTransaction( addedInTx ) );
            assertFalse( tx.dataRead().nodeDeletedInTransaction( unaffected ) );
            assertTrue( tx.dataRead().nodeDeletedInTransaction( addedAndRemovedInTx ) );
            assertTrue( tx.dataRead().nodeDeletedInTransaction( deletedInTx ) );
        }
    }

    @Test
    public void shouldDetectRelationshipDeletedInTransaction() throws Exception
    {
        // GIVEN
        long node;
        int relType;
        long deletedInTx, unaffected, addedInTx, addedAndRemovedInTx;
        try ( Transaction tx = beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            relType = tx.tokenWrite().relationshipTypeCreateForName( "REL_TYPE" );
            deletedInTx = tx.dataWrite().relationshipCreate(node, relType, node);
            unaffected = tx.dataWrite().relationshipCreate(node, relType, node);
            tx.success();
        }

        try ( Transaction tx = beginTransaction() )
        {
            // WHEN
            addedInTx = tx.dataWrite().relationshipCreate(node, relType, node);
            addedAndRemovedInTx = tx.dataWrite().relationshipCreate(node, relType, node);
            tx.dataWrite().relationshipDelete( deletedInTx );
            tx.dataWrite().relationshipDelete( addedAndRemovedInTx );

            // THEN
            assertFalse( tx.dataRead().relationshipDeletedInTransaction( addedInTx ) );
            assertFalse( tx.dataRead().relationshipDeletedInTransaction( unaffected ) );
            assertTrue( tx.dataRead().relationshipDeletedInTransaction( addedAndRemovedInTx ) );
            assertTrue( tx.dataRead().relationshipDeletedInTransaction( deletedInTx ) );
        }
    }

    @Test
    public void shouldReportInTransactionNodeProperty() throws Exception
    {
        // GIVEN
        long node;
        int p1, p2, p3, p4, p5;
        try ( Transaction tx = beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            p1 = tx.tokenWrite().propertyKeyCreateForName( "p1" );
            p2 = tx.tokenWrite().propertyKeyCreateForName( "p2" );
            p3 = tx.tokenWrite().propertyKeyCreateForName( "p3" );
            p4 = tx.tokenWrite().propertyKeyCreateForName( "p4" );
            p5 = tx.tokenWrite().propertyKeyCreateForName( "p5" );
            tx.dataWrite().nodeSetProperty( node, p1, Values.of( 1 ) );
            tx.dataWrite().nodeSetProperty( node, p3, Values.of( 3 ) );
            tx.dataWrite().nodeSetProperty( node, p4, Values.of( 4 ) );
            tx.success();
        }

        try ( Transaction tx = beginTransaction() )
        {
            // WHEN
            tx.dataWrite().nodeSetProperty( node, p3, Values.of( 13 ) );
            tx.dataWrite().nodeRemoveProperty( node, p4 );
            tx.dataWrite().nodeSetProperty( node, p5, Values.of( 15 ) );

            // THEN
            assertNull( "Unchanged existing property is null",
                        tx.dataRead().nodePropertyChangeInTransactionOrNull( node, p1 ) );

            assertNull( "Unchanged missing property is null",
                        tx.dataRead().nodePropertyChangeInTransactionOrNull( node, p2 ) );

            assertEquals( "Changed property is new value", Values.of( 13 ),
                          tx.dataRead().nodePropertyChangeInTransactionOrNull( node, p3 ) );

            assertEquals( "Removed property is NO_VALUE", Values.NO_VALUE,
                          tx.dataRead().nodePropertyChangeInTransactionOrNull( node, p4 ) );

            assertEquals( "Added property is new value", Values.of( 15 ),
                          tx.dataRead().nodePropertyChangeInTransactionOrNull( node, p5 ) );
        }
    }
}
