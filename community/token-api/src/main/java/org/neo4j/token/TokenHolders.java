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
package org.neo4j.token;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.token.api.TokensLoader;

/**
 * Holds onto all available {@link TokenHolder} for easily passing all those around
 * and for easily extending available instances in one place.
 * Resolves token names without going through transaction and locking layers.
 */
public class TokenHolders implements TokenNameLookup
{
    private final TokenHolder propertyKeyTokens;
    private final TokenHolder labelTokens;
    private final TokenHolder relationshipTypeTokens;

    public TokenHolders( TokenHolder propertyKeyTokens, TokenHolder labelTokens, TokenHolder relationshipTypeTokens )
    {
        this.propertyKeyTokens = propertyKeyTokens;
        this.labelTokens = labelTokens;
        this.relationshipTypeTokens = relationshipTypeTokens;
    }

    public TokenHolder propertyKeyTokens()
    {
        return propertyKeyTokens;
    }

    public TokenHolder labelTokens()
    {
        return labelTokens;
    }

    public TokenHolder relationshipTypeTokens()
    {
        return relationshipTypeTokens;
    }

    public void setInitialTokens( TokensLoader loader, PageCursorTracer cursorTracer )
    {
        propertyKeyTokens().setInitialTokens( loader.getPropertyKeyTokens( cursorTracer ) );
        labelTokens().setInitialTokens( loader.getLabelTokens( cursorTracer ) );
        relationshipTypeTokens().setInitialTokens( loader.getRelationshipTypeTokens( cursorTracer ) );
    }

    @Override
    public String labelGetName( int labelId )
    {
        try
        {
            return labelTokens().getTokenById( labelId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            return "[no such label: " + labelId + ']';
        }
    }

    @Override
    public String relationshipTypeGetName( int relationshipTypeId )
    {
        try
        {
            return relationshipTypeTokens().getTokenById( relationshipTypeId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            return "[no such relationship type: " + relationshipTypeId + ']';
        }
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId )
    {
        try
        {
            return propertyKeyTokens().getTokenById( propertyKeyId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            return "[no such property key: " + propertyKeyId + ']';
        }
    }

    /**
     * Produce a {@link TokenNameLookup} that appends the token ids to all the token names.
     */
    public TokenNameLookup lookupWithIds()
    {
        return new TokenNameLookup()
        {
            @Override
            public String labelGetName( int labelId )
            {
                return TokenHolders.this.labelGetName( labelId ) + '[' + labelId + ']';
            }

            @Override
            public String relationshipTypeGetName( int relationshipTypeId )
            {
                return TokenHolders.this.relationshipTypeGetName( relationshipTypeId ) + '[' + relationshipTypeId + ']';
            }

            @Override
            public String propertyKeyGetName( int propertyKeyId )
            {
                return TokenHolders.this.propertyKeyGetName( propertyKeyId ) + '[' + propertyKeyId + ']';
            }
        };
    }
}
