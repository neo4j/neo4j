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

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@EnableRuleMigrationSupport
public class InMemoryTokenCacheTest
{
    private static final String INBOUND2_TYPE = "inbound2";
    private static final String INBOUND1_TYPE = "inbound1";
    @Rule
    public ExpectedException expectedEcxeption = ExpectedException.none();

    @Test
    public void addTokenWithDuplicatedNotAllowed()
    {
        InMemoryTokenCache<RelationshipTypeToken> tokenCache = createTokenCache();
        tokenCache.put( new RelationshipTypeToken( INBOUND1_TYPE, 1 ) );
        tokenCache.put( new RelationshipTypeToken( INBOUND2_TYPE, 2 ) );

        expectedEcxeption.expect( NonUniqueTokenException.class );
        expectedEcxeption.expectMessage( "The testType \"inbound1\" is not unique" );

        tokenCache.put( new RelationshipTypeToken( INBOUND1_TYPE, 3 ) );
    }

    @Test
    public void keepOriginalTokenWhenAddDuplicate()
    {
        InMemoryTokenCache<RelationshipTypeToken> tokenCache = createTokenCache();
        tokenCache.put( new RelationshipTypeToken( INBOUND1_TYPE, 1 ) );
        tokenCache.put( new RelationshipTypeToken( INBOUND2_TYPE, 2 ) );

        tryToAddDuplicate( tokenCache );

        assertEquals( 1, tokenCache.getId( INBOUND1_TYPE ).intValue() );
        assertEquals( 2, tokenCache.getId( INBOUND2_TYPE ).intValue() );
        assertNull( tokenCache.getToken( 3 ) );
    }

    private InMemoryTokenCache<RelationshipTypeToken> createTokenCache()
    {
        return new InMemoryTokenCache<>( "testType" );
    }

    private void tryToAddDuplicate( InMemoryTokenCache<RelationshipTypeToken> tokenCache )
    {
        try
        {
            tokenCache.put( new RelationshipTypeToken( INBOUND1_TYPE, 3 ) );
        }
        catch ( NonUniqueTokenException ignored )
        {
        }
    }

}
