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
package org.neo4j.kernel.impl.core;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DelegatingTokenHolderTest
{
    private TokenCreator creator;
    private TokenHolder holder;

    @Before
    public void setUp() throws Exception
    {
        creator = mock( TokenCreator.class );
        holder = new DelegatingTokenHolder( creator, "Dummy" );
    }

    @Test
    public void mustCreateAndCacheNewTokens() throws Exception
    {
        when( creator.createToken( "token" ) ).thenReturn( 42 );
        assertThat( holder.getOrCreateId( "token" ), is( 42 ) );
        assertThat( holder.getOrCreateId( "token" ), is( 42 ) );
        // Verify implies that the call only happens once.
        verify( creator ).createToken( "token" );
        verifyNoMoreInteractions( creator );
    }

    @Test
    public void batchTokenGetMustReturnWhetherThereWereUnresolvedTokens()
    {
        holder.setInitialTokens( asList(
                token( "a", 1 ),
                token( "b", 2 ) ) );
        String[] names;
        int[] ids;

        names = new String[]{"a", "X", "b"};
        ids = new int[]{-1, -1, -1};
        assertTrue( holder.getIdsByNames( names, ids ) );
        assertThat( ids[0], is( 1 ) );
        assertThat( ids[1], is( -1 ) );
        assertThat( ids[2], is( 2 ) );

        names = new String[]{"a", "b"};
        ids = new int[]{-1, -1};
        assertFalse( holder.getIdsByNames( names, ids ) );
        assertThat( ids[0], is( 1 ) );
        assertThat( ids[1], is( 2 ) );
    }

    @Test
    public void batchTokenCreateMustIgnoreExistingTokens() throws Exception
    {
        initialTokensABC();

        AtomicInteger nextId = new AtomicInteger( 42 );
        mockAssignNewTokenIdsInBatch( nextId );

        String[] names = new String[]{"b", "X", "a", "Y", "c"};
        int[] ids = new int[names.length];
        holder.getOrCreateIds( names, ids );
        assertThat( ids.length, is( 5 ) );
        assertThat( ids[0], is( 2 ) );
        assertThat( ids[1], isOneOf( 42, 43 ) );
        assertThat( ids[2], is( 1 ) );
        assertThat( ids[3], isOneOf( 42, 43 ) );
        assertThat( ids[4], is( 3 ) );
        assertThat( nextId.get(), is( 44 ) );

        // And these should not throw.
        holder.getTokenById( 42 );
        holder.getTokenById( 43 );
    }

    private void mockAssignNewTokenIdsInBatch( AtomicInteger nextId ) throws KernelException
    {
        doAnswer( inv ->
        {
            int[] ids = inv.getArgument( 1 );
            IntPredicate filter = inv.getArgument( 2 );
            for ( int i = 0; i < ids.length; i++ )
            {
                if ( filter.test( i ) )
                {
                    ids[i] = nextId.getAndIncrement();
                }
            }
            return null;
        } ).when( creator ).createTokens( any( String[].class ), any( int[].class ), any( IntPredicate.class ) );
    }

    private void initialTokensABC() throws KernelException
    {
        holder.setInitialTokens( asList(
                token( "a", 1 ),
                token( "b", 2 ) ) );

        when( creator.createToken( "c" ) ).thenReturn( 3 );
        assertThat( holder.getOrCreateId( "c" ), is( 3 ) );
    }

    @Test
    public void batchTokenCreateMustDeduplicateTokenCreates() throws Exception
    {
        initialTokensABC();

        AtomicInteger nextId = new AtomicInteger( 42 );
        mockAssignNewTokenIdsInBatch( nextId );

        // NOTE: the existing 'b', and the missing 'X', tokens are in here twice:
        String[] names = new String[]{"b", "b", "X", "a", "X", "c"};
        int[] ids = new int[names.length];
        holder.getOrCreateIds( names, ids );

        assertThat( ids.length, is( 6 ) );
        assertThat( ids[0], is( 2 ) );
        assertThat( ids[1], is( 2 ) );
        assertThat( ids[2], is( 42 ) );
        assertThat( ids[3], is( 1 ) );
        assertThat( ids[4], is( 42 ) );
        assertThat( ids[5], is( 3 ) );
        assertThat( nextId.get(), is( 43 ) );

        // And this should not throw.
        holder.getTokenById( 42 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void batchTokenCreateMustThrowOnArraysOfDifferentLengths()
    {
        holder.getOrCreateIds( new String[3], new int[2] );
    }

    @Test
    public void shouldClearTokensAsPartOfInitialTokenLoading()
    {
        // GIVEN
        holder.setInitialTokens( asList(
                token( "one", 1 ),
                token( "two", 2 ) ) );
        assertTokens( holder.getAllTokens(),
                token( "one", 1 ),
                token( "two", 2 ) );

        // WHEN
        holder.setInitialTokens( asList(
                token( "two", 2 ),
                token( "three", 3 ),
                token( "four", 4 ) ) );

        // THEN
        assertTokens( holder.getAllTokens(),
                token( "two", 2 ),
                token( "three", 3 ),
                token( "four", 4 ) );
    }

    private void assertTokens( Iterable<NamedToken> allTokens, NamedToken... expectedTokens )
    {
        Map<String,NamedToken> existing = new HashMap<>();
        for ( NamedToken token : allTokens )
        {
            existing.put( token.name(), token );
        }
        Map<String,NamedToken> expected = new HashMap<>();
        for ( NamedToken token : expectedTokens )
        {
            expected.put( token.name(), token );
        }
        assertEquals( expected, existing );
    }

    private NamedToken token( String name, int id )
    {
        return new NamedToken( name, id );
    }
}
