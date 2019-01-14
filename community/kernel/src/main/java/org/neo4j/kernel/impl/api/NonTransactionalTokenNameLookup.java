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

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.core.TokenNotFoundException;

import static java.lang.String.format;

/**
 * A token name resolver that directly accesses the databases cached property and label tokens, bypassing
 * the transactional and locking layers.
 */
public class NonTransactionalTokenNameLookup implements TokenNameLookup
{
    private final TokenHolders tokenHolders;

    public NonTransactionalTokenNameLookup( TokenHolders tokenHolders )
    {
        this.tokenHolders = tokenHolders;
    }

    @Override
    public String labelGetName( int labelId )
    {
        return tokenById( tokenHolders.labelTokens(), labelId, "label" );
    }

    @Override
    public String relationshipTypeGetName( int relTypeId )
    {
        return tokenById( tokenHolders.relationshipTypeTokens(), relTypeId, "relationshipType" );
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId )
    {
        return tokenById( tokenHolders.propertyKeyTokens(), propertyKeyId, "property" );
    }

    private static String tokenById( TokenHolder tokenHolder, int tokenId, String tokenName )
    {
        try
        {
            return tokenHolder.getTokenById( tokenId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            // Ignore errors from reading key
        }
        return format( "%s[%d]", tokenName, tokenId );
    }
}
