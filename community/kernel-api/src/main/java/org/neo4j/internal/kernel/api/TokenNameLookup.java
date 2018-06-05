/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.internal.kernel.api;

import java.util.function.IntFunction;

import org.neo4j.storageengine.api.EntityType;

/**
 * Lookup of names from token ids. Tokens are mostly referred to by ids throughout several abstractions.
 * Sometimes token names are required, this is a way to lookup names in those cases.
 */
public interface TokenNameLookup
{
    /**
     * @param labelId id of label to get name for.
     * @return name of label token with given id.
     */
    String labelGetName( int labelId );

    /**
     * @param relationshipTypeId id of relationship type to get name for.
     * @return name of relationship type token with given id.
     */
    String relationshipTypeGetName( int relationshipTypeId );

    /**
     * @param propertyKeyId id of property key to get name for.
     * @return name of property key token with given id.
     */
    String propertyKeyGetName( int propertyKeyId );

    default String[] entityTokensGetNames( EntityType type, int[] entityTokenIds )
    {
        IntFunction<String> mapper;
        switch ( type )
        {
        case NODE:
            mapper = this::labelGetName;
            break;
        case RELATIONSHIP:
            mapper = this::relationshipTypeGetName;
            break;
        default:
            throw new IllegalArgumentException( "Cannot lookup names for tokens of type: " + type );
        }
        String[] tokenNames = new String[entityTokenIds.length];
        for ( int i = 0; i < entityTokenIds.length; i++ )
        {
            tokenNames[i] = mapper.apply( entityTokenIds[i] );
        }
        return tokenNames;    }
}
