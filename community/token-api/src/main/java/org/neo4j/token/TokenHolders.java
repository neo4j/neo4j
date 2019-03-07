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
package org.neo4j.token;

import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokensLoader;

/**
 * Holds onto all available {@link TokenHolder} for easily passing all those around
 * and for easily extending available instances in one place.
 */
public class TokenHolders
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

    public void setInitialTokens( TokensLoader loader )
    {
        propertyKeyTokens().setInitialTokens( loader.getPropertyKeyTokens() );
        labelTokens().setInitialTokens( loader.getLabelTokens() );
        relationshipTypeTokens().setInitialTokens( loader.getRelationshipTypeTokens() );
    }

    /**
     * @return TokenHolders which can be initialized, but not have any new tokens created.
     */
    public static TokenHolders readOnlyTokenHolders()
    {
        return new TokenHolders(
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
    }
}
