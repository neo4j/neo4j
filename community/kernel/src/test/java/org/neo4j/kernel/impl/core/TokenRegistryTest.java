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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.internal.kernel.api.NamedToken;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TokenRegistryTest
{
    private static final String INBOUND2_TYPE = "inbound2";
    private static final String INBOUND1_TYPE = "inbound1";
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void addTokenWithDuplicatedNotAllowed()
    {
        TokenRegistry tokenCache = createTokenCache();
        tokenCache.put( new NamedToken( INBOUND1_TYPE, 1 ) );
        tokenCache.put( new NamedToken( INBOUND2_TYPE, 2 ) );

        expectedException.expect( NonUniqueTokenException.class );
        expectedException.expectMessage( "The testType \"inbound1\" is not unique" );

        tokenCache.put( new NamedToken( INBOUND1_TYPE, 3 ) );
    }

    @Test
    public void keepOriginalTokenWhenAddDuplicate()
    {
        TokenRegistry tokenCache = createTokenCache();
        tokenCache.put( new NamedToken( INBOUND1_TYPE, 1 ) );
        tokenCache.put( new NamedToken( INBOUND2_TYPE, 2 ) );

        tryToAddDuplicate( tokenCache );

        assertEquals( 1, tokenCache.getId( INBOUND1_TYPE ).intValue() );
        assertEquals( 2, tokenCache.getId( INBOUND2_TYPE ).intValue() );
        assertNull( tokenCache.getToken( 3 ) );
    }

    private TokenRegistry createTokenCache()
    {
        return new TokenRegistry( "testType" );
    }

    private void tryToAddDuplicate( TokenRegistry tokenCache )
    {
        try
        {
            tokenCache.put( new NamedToken( INBOUND1_TYPE, 3 ) );
        }
        catch ( NonUniqueTokenException ignored )
        {
        }
    }

}
