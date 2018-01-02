/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TokenHolderTest
{
    @Test
    public void shouldClearTokensAsPartOfInitialTokenLoading() throws Exception
    {
        // GIVEN
        TokenCreator creator = mock( TokenCreator.class );
        TokenHolder<Token> holder = new DelegatingTokenHolder<Token>( creator, new Token.Factory() ) {};
        holder.setInitialTokens(
                asList( token( "one", 1 ),
                        token( "two", 2 ) ));
        assertTokens( holder.getAllTokens(),
                token( "one", 1 ),
                token( "two", 2 ) );

        // WHEN
        holder.setInitialTokens(asList(
                token( "two", 2 ),
                token( "three", 3 ),
                token( "four", 4 ) ));

        // THEN
        assertTokens( holder.getAllTokens(),
                token( "two", 2 ),
                token( "three", 3 ),
                token( "four", 4 ) );
    }

    private void assertTokens( Iterable<Token> allTokens, Token... expectedTokens )
    {
        Map<String,Token> existing = new HashMap<>();
        for ( Token token : allTokens )
        {
            existing.put( token.name(), token );
        }
        Map<String,Token> expected = new HashMap<>();
        for ( Token token : expectedTokens )
        {
            expected.put( token.name(), token );
        }
        assertEquals( expected, existing );
    }

    private Token token( String name, int id )
    {
        return new Token( name, id );
    }
}
