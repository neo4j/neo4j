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
package org.neo4j.kernel.impl.api;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.kernel.impl.locking.LockGroup;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchTransactionApplierFacadeTest
{

    private BatchTransactionApplierFacade facade;
    private BatchTransactionApplier applier1;
    private BatchTransactionApplier applier2;
    private BatchTransactionApplier applier3;
    private TransactionApplier txApplier1;
    private TransactionApplier txApplier2;
    private TransactionApplier txApplier3;

    @Before
    public void setUp() throws Exception
    {
        txApplier1 = mock( TransactionApplier.class );
        applier1 = mock( BatchTransactionApplier.class );
        when( applier1.startTx( any( TransactionToApply.class ) ) ).thenReturn( txApplier1 );
        when( applier1.startTx( any( TransactionToApply.class ), any( LockGroup.class ) ) ).thenReturn( txApplier1 );

        txApplier2 = mock( TransactionApplier.class );
        applier2 = mock( BatchTransactionApplier.class );
        when( applier2.startTx( any( TransactionToApply.class ) ) ).thenReturn( txApplier2 );
        when( applier2.startTx( any( TransactionToApply.class ), any( LockGroup.class ) ) ).thenReturn( txApplier2 );

        txApplier3 = mock( TransactionApplier.class );
        applier3 = mock( BatchTransactionApplier.class );
        when( applier3.startTx( any( TransactionToApply.class ) ) ).thenReturn( txApplier3 );
        when( applier3.startTx( any( TransactionToApply.class ), any( LockGroup.class ) ) ).thenReturn( txApplier3 );

        facade = new BatchTransactionApplierFacade( applier1, applier2, applier3 );
    }

    @Test
    public void testStartTxCorrectOrder() throws Exception
    {
        // GIVEN
        TransactionToApply tx = mock( TransactionToApply.class );

        // WHEN
        TransactionApplierFacade result = (TransactionApplierFacade) facade.startTx( tx );

        // THEN
        InOrder inOrder = inOrder( applier1, applier2, applier3 );

        inOrder.verify( applier1 ).startTx( tx );
        inOrder.verify( applier2 ).startTx( tx );
        inOrder.verify( applier3 ).startTx( tx );

        assertEquals( txApplier1, result.appliers[0] );
        assertEquals( txApplier2, result.appliers[1] );
        assertEquals( txApplier3, result.appliers[2] );
        assertEquals( 3, result.appliers.length );
    }

    @Test
    public void testStartTxCorrectOrderWithLockGroup() throws Exception
    {
        // GIVEN
        TransactionToApply tx = mock( TransactionToApply.class );
        LockGroup lockGroup = mock( LockGroup.class );

        // WHEN
        TransactionApplierFacade result = (TransactionApplierFacade) facade.startTx( tx, lockGroup );

        // THEN
        InOrder inOrder = inOrder( applier1, applier2, applier3 );

        inOrder.verify( applier1 ).startTx( tx, lockGroup );
        inOrder.verify( applier2 ).startTx( tx, lockGroup );
        inOrder.verify( applier3 ).startTx( tx, lockGroup );

        assertEquals( txApplier1, result.appliers[0] );
        assertEquals( txApplier2, result.appliers[1] );
        assertEquals( txApplier3, result.appliers[2] );
        assertEquals( 3, result.appliers.length );
    }

    @Test
    public void closeShouldBeDoneInReverseOrder() throws Exception
    {
        // No idea why it was done like this before refactoring

        // WHEN
        facade.close();

        // THEN
        InOrder inOrder = inOrder( applier1, applier2, applier3 );

        inOrder.verify( applier3 ).close();
        inOrder.verify( applier2 ).close();
        inOrder.verify( applier1 ).close();
    }
}
