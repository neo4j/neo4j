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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.core.TokenHolder;

import static java.lang.String.format;

/**
 * A token name resolver that directly accesses the databases cached property and label tokens, bypassing
 * the transactional and locking layers.
 */
public class NonTransactionalTokenNameLookup implements TokenNameLookup
{
    private final LabelTokenHolder labelTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;

    public NonTransactionalTokenNameLookup( LabelTokenHolder labelTokenHolder,
            RelationshipTypeTokenHolder relationshipTypeTokenHolder,
            PropertyKeyTokenHolder propertyKeyTokenHolder )
    {
        this.labelTokenHolder = labelTokenHolder;
        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
    }

    @Override
    public String labelGetName( int labelId )
    {
        return tokenById( labelTokenHolder, labelId, "label" );
    }

    @Override
    public String relationshipTypeGetName( int relTypeId )
    {
        return tokenById( relationshipTypeTokenHolder, relTypeId, "relationshipType" );
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId )
    {
        return tokenById( propertyKeyTokenHolder, propertyKeyId, "property" );
    }

    private static String tokenById( TokenHolder tokenHolder, int tokenId, String tokenName )
    {
        try
        {
            Token token = tokenHolder.getTokenByIdOrNull( tokenId );
            if ( token != null )
            {
                return token.name();
            }
        }
        catch ( RuntimeException e )
        {
            // Ignore errors from reading key
        }
        return format( "%s[%d]", tokenName, tokenId );
    }
}
