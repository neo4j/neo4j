/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.Test;

import org.neo4j.function.ThrowingAction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.storageengine.api.StorageReader;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;
import static org.neo4j.test.MockedNeoStores.mockedTokenHolders;
import static org.neo4j.test.assertion.Assert.assertException;

public class KernelTokenTest
{
    private final StorageReader storeReadLayer = mock( StorageReader.class );
    private final KernelTransactionImplementation ktx = mock( KernelTransactionImplementation.class );
    private final TokenHolders tokenHolders = mockedTokenHolders();
    private KernelToken token = new KernelToken( storeReadLayer, ktx, tokenHolders );

    @Test
    public void labelGetOrCreateForName() throws Exception
    {
        assertIllegalToken( () -> token.labelGetOrCreateForName( null ) );
        assertIllegalToken( () -> token.labelGetOrCreateForName( "" ) );
        when( tokenHolders.labelTokens().getIdByName( "label" ) ).thenReturn( NO_TOKEN );
        when( tokenHolders.labelTokens().getOrCreateId( "label" ) ).thenReturn( 42 );
        assertThat( token.labelGetOrCreateForName( "label" ), is( 42 ) );
    }

    @Test
    public void labelGetOrCreateForNames() throws Exception
    {
        assertIllegalToken( () -> token.labelGetOrCreateForNames( new String[]{null}, new int[1] ) );
        assertIllegalToken( () -> token.labelGetOrCreateForNames( new String[]{""}, new int[1] ) );
        String[] names = {"a", "b"};
        int[] ids = new int[2];
        when( tokenHolders.labelTokens().getIdByName( "a" ) ).thenReturn( NO_TOKEN );
        token.labelGetOrCreateForNames( names, ids );
        verify( tokenHolders.labelTokens() ).getOrCreateIds( names, ids );
    }

    @Test
    public void propertyKeyGetOrCreateForName() throws IllegalTokenNameException
    {
        assertIllegalToken( () -> token.propertyKeyGetOrCreateForName( null ) );
        assertIllegalToken( () -> token.propertyKeyGetOrCreateForName( "" ) );
        when( tokenHolders.propertyKeyTokens().getIdByName( "prop" ) ).thenReturn( NO_TOKEN );
        when( tokenHolders.propertyKeyTokens().getOrCreateId( "prop" ) ).thenReturn( 42 );
        assertThat( token.propertyKeyGetOrCreateForName( "prop" ), is( 42 ) );
    }

    @Test
    public void propertyGetOrCreateForNames() throws Exception
    {
        assertIllegalToken( () -> token.propertyKeyGetOrCreateForNames( new String[]{null}, new int[1] ) );
        assertIllegalToken( () -> token.propertyKeyGetOrCreateForNames( new String[]{""}, new int[1] ) );
        String[] names = {"a", "b"};
        int[] ids = new int[2];
        when( tokenHolders.propertyKeyTokens().getIdByName( "a" ) ).thenReturn( NO_TOKEN );
        token.propertyKeyGetOrCreateForNames( names, ids );
        verify( tokenHolders.propertyKeyTokens() ).getOrCreateIds( names, ids );
    }

    @Test
    public void relationshipTypeGetOrCreateForName() throws IllegalTokenNameException
    {
        assertIllegalToken( () -> token.relationshipTypeGetOrCreateForName( null ) );
        assertIllegalToken( () -> token.relationshipTypeGetOrCreateForName( "" ) );
        when( tokenHolders.relationshipTypeTokens().getIdByName( "rel" ) ).thenReturn( NO_TOKEN );
        when( tokenHolders.relationshipTypeTokens().getOrCreateId( "rel" ) ).thenReturn( 42 );
        assertThat( token.relationshipTypeGetOrCreateForName( "rel" ), is( 42 ) );
    }

    @Test
    public void relationshipTypeOrCreateForNames() throws Exception
    {
        assertIllegalToken( () -> token.relationshipTypeGetOrCreateForNames( new String[]{null}, new int[1] ) );
        assertIllegalToken( () -> token.relationshipTypeGetOrCreateForNames( new String[]{""}, new int[1] ) );
        String[] names = {"a", "b"};
        int[] ids = new int[2];
        when( tokenHolders.relationshipTypeTokens().getIdByName( "a" ) ).thenReturn( NO_TOKEN );
        token.relationshipTypeGetOrCreateForNames( names, ids );
        verify( tokenHolders.relationshipTypeTokens() ).getOrCreateIds( names, ids );
    }

    private void assertIllegalToken( ThrowingAction<KernelException> f )
    {
        assertException( f, IllegalTokenNameException.class );
    }
}
