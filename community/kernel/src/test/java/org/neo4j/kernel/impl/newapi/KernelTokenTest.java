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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KernelTokenTest
{
    private KernelTransactionImplementation ktx;
    private TransactionState transactionState;
    private CommandCreationContext commandCreationContext;
    private KernelToken kernelToken;
    private StorageReader storageReader;
    private TokenHolders tokenHolders;
    private TokenHolder propertyKeyTokens;
    private TokenHolder labelTokens;
    private TokenHolder relationshipTypeTokens;

    @BeforeEach
    void setUp()
    {
        ktx = mock( KernelTransactionImplementation.class );
        transactionState = mock( TransactionState.class );
        when( ktx.txState() ).thenReturn( transactionState );
        commandCreationContext = mock( CommandCreationContext.class );
        storageReader = mock( StorageReader.class );
        propertyKeyTokens = mock( TokenHolder.class );
        labelTokens = mock( TokenHolder.class );
        relationshipTypeTokens = mock( TokenHolder.class );
        tokenHolders = new TokenHolders( propertyKeyTokens, labelTokens, relationshipTypeTokens );
        kernelToken = new KernelToken( storageReader, commandCreationContext, ktx, tokenHolders );
    }

    @Test
    void shouldAcquireTxStateBeforeAllocatingLabelTokenId() throws KernelException
    {
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
        // when
        kernelToken.relationshipTypeCreateForName( "MyType", false );

        // then
        InOrder inOrder = inOrder( ktx, commandCreationContext );
        inOrder.verify( ktx ).txState();
        inOrder.verify( commandCreationContext ).reserveRelationshipTypeTokenId();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void invalidTokenNamesAreNotAllowed()
    {
        List<String> invalidNames = List.of( "", "\0", "`", "``", "`a`", "a`b", "a``b" );

        assertThrows( IllegalTokenNameException.class, () -> kernelToken.labelGetOrCreateForName( null ),
                "label name should be invalid: null" );
        assertThrows( IllegalTokenNameException.class, () -> kernelToken.relationshipTypeGetOrCreateForName( null ),
                "relationship type name should be invalid: null" );
        assertThrows( IllegalTokenNameException.class, () -> kernelToken.propertyKeyGetOrCreateForName( null ),
                "property key name should be invalid: null" );

        for ( String invalidName : invalidNames )
        {
            assertThrows( IllegalTokenNameException.class, () -> kernelToken.labelGetOrCreateForName( invalidName ),
                    "label name should be invalid: '" + invalidName + "'" );
            assertThrows( IllegalTokenNameException.class, () -> kernelToken.relationshipTypeGetOrCreateForName( invalidName ),
                    "relationship type name should be invalid: '" + invalidName + "'" );
            assertThrows( IllegalTokenNameException.class, () -> kernelToken.propertyKeyGetOrCreateForName( invalidName ),
                    "property key name name should be invalid: '" + invalidName + "'" );
        }
    }

    @Test
    void allowedSpecialCharactersInTokenNames() throws KernelException
    {
        List<String> validFancyTokenNames = List.of( "\t", " ", "  ", "\n", "\r", "\uD83D\uDE02", "\"", "'", "%", "@", "#", "$", "{", "}" );

        for ( String validName : validFancyTokenNames )
        {
            kernelToken.labelGetOrCreateForName( validName );
            kernelToken.relationshipTypeGetOrCreateForName( validName );
            kernelToken.propertyKeyGetOrCreateForName( validName );
        }
    }
}
