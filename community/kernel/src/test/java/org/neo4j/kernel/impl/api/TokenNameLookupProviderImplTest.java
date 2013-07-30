/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.operations.TokenNameLookup;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TokenNameLookupProviderImplTest
{
    @SuppressWarnings("unchecked")
    @Test
    public void shouldRunUsingExecutor() throws Exception
    {
        // GIVEN
        Transactor transactor = mock( Transactor.class );
        TokenNameLookupProviderImpl provider = new TokenNameLookupProviderImpl( transactor );

        // WHEN
        provider.withTokenNameLookup( new Function<TokenNameLookup, Void>()
        {
            @Override
            public Void apply( TokenNameLookup tokenNameLookup )
            {
                return null;
            }
        } );

        // THEN
        verify( transactor ).execute( any( Transactor.Statement.class ) );
    }
}
