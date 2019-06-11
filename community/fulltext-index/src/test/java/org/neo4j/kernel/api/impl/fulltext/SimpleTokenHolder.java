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
package org.neo4j.kernel.api.impl.fulltext;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.kernel.impl.core.AbstractTokenHolderBase;
import org.neo4j.kernel.impl.core.TokenRegistry;

class SimpleTokenHolder extends AbstractTokenHolderBase
{
    private int nextId;

    SimpleTokenHolder( TokenRegistry tokenRegistry )
    {
        super( tokenRegistry );
    }

    @Override
    protected int createToken( String tokenName )
    {
        return nextId++;
    }

    @Override
    public void getOrCreateIds( String[] names, int[] ids )
    {
        for ( int i = 0; i < names.length; i++ )
        {
            ids[i] = getOrCreateId( names[i] );
        }
    }

    static TokenRegistry createPopulatedTokenRegistry( String tokenType, int[] tokenIds )
    {
        TokenRegistry tokenRegistry = new TokenRegistry( tokenType );
        List<NamedToken> tokens = new ArrayList<>();
        for ( int propertyId : tokenIds )
        {
            tokens.add( new NamedToken( tokenType + propertyId, propertyId ) );
        }
        tokenRegistry.setInitialTokens( tokens );
        return tokenRegistry;
    }
}
