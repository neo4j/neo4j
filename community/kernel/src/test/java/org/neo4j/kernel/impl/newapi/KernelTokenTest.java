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
import org.mockito.InOrder;

import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KernelTokenTest
{
    @Test
    void shouldAcquireTxStateBeforeAllocatingLabelTokenId() throws KernelException
    {
        // given
        KernelTransactionImplementation ktx = mock( KernelTransactionImplementation.class );
        when( ktx.txState() ).thenReturn( mock( TransactionState.class ) );
        CommandCreationContext commandCreationContext = mock( CommandCreationContext.class );
        KernelToken kernelToken = new KernelToken( mock( StorageReader.class ), commandCreationContext, ktx,
                new TokenHolders( mock( TokenHolder.class ), mock( TokenHolder.class ), mock( TokenHolder.class ) ) );

        // when
        kernelToken.labelCreateForName( "MyLabel", false );

        // then
        InOrder inOrder = inOrder( ktx, commandCreationContext );
        inOrder.verify( ktx ).txState();
        inOrder.verify( commandCreationContext ).reserveLabelTokenId();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireTxStateBeforeAllocatingPropertyKeyTokenId() throws KernelException
    {
        // given
        KernelTransactionImplementation ktx = mock( KernelTransactionImplementation.class );
        when( ktx.txState() ).thenReturn( mock( TransactionState.class ) );
        CommandCreationContext commandCreationContext = mock( CommandCreationContext.class );
        KernelToken kernelToken = new KernelToken( mock( StorageReader.class ), commandCreationContext, ktx,
                new TokenHolders( mock( TokenHolder.class ), mock( TokenHolder.class ), mock( TokenHolder.class ) ) );

        // when
        kernelToken.propertyKeyCreateForName( "MyKey", false );

        // then
        InOrder inOrder = inOrder( ktx, commandCreationContext );
        inOrder.verify( ktx ).txState();
        inOrder.verify( commandCreationContext ).reservePropertyKeyTokenId();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireTxStateBeforeAllocatingRelationshipTypeTokenId() throws KernelException
    {
        // given
        KernelTransactionImplementation ktx = mock( KernelTransactionImplementation.class );
        when( ktx.txState() ).thenReturn( mock( TransactionState.class ) );
        CommandCreationContext commandCreationContext = mock( CommandCreationContext.class );
        KernelToken kernelToken = new KernelToken( mock( StorageReader.class ), commandCreationContext, ktx,
                new TokenHolders( mock( TokenHolder.class ), mock( TokenHolder.class ), mock( TokenHolder.class ) ) );

        // when
        kernelToken.relationshipTypeCreateForName( "MyType", false );

        // then
        InOrder inOrder = inOrder( ktx, commandCreationContext );
        inOrder.verify( ktx ).txState();
        inOrder.verify( commandCreationContext ).reserveRelationshipTypeTokenId();
        inOrder.verifyNoMoreInteractions();
    }
}
