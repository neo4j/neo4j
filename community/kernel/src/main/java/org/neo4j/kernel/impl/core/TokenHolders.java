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
package org.neo4j.kernel.impl.core;

import org.neo4j.kernel.impl.store.NeoStores;

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

    private void setInitialTokens( NeoStores neoStores )
    {
        propertyKeyTokens().setInitialTokens( neoStores.getPropertyKeyTokenStore().getTokens() );
        labelTokens().setInitialTokens( neoStores.getLabelTokenStore().getTokens() );
        relationshipTypeTokens().setInitialTokens( neoStores.getRelationshipTypeTokenStore().getTokens() );
    }

    public static TokenHolders readOnlyTokenHolders( NeoStores neoStores )
    {
        TokenHolder propertyKeyTokens1 = createReadOnlyTokenHolder( TokenHolder.TYPE_PROPERTY_KEY );
        TokenHolder labelTokens1 = createReadOnlyTokenHolder( TokenHolder.TYPE_LABEL );
        TokenHolder relationshipTypeTokens1 = createReadOnlyTokenHolder( TokenHolder.TYPE_RELATIONSHIP_TYPE );
        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokens1, labelTokens1, relationshipTypeTokens1 );
        tokenHolders.setInitialTokens( neoStores );
        return tokenHolders;
    }

    private static TokenHolder createReadOnlyTokenHolder( String tokenType )
    {
        return new DelegatingTokenHolder( new ReadOnlyTokenCreator(), tokenType );
    }
}
