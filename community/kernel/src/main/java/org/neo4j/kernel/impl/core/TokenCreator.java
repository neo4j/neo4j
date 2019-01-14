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

import java.util.function.IntPredicate;

import org.neo4j.internal.kernel.api.exceptions.KernelException;

public interface TokenCreator
{
    /**
     * Create a token by the given name and return the newly allocated id for this token.
     * <p>
     * It is assumed that the token name is not already being used.
     *
     * @param name The token name to allocate.
     * @return The id of the allocated token name.
     * @throws KernelException If the inner transaction used to allocate the token encountered a problem.
     */
    int createToken( String name ) throws KernelException;

    /**
     * Create the tokens by the given names, and store their ids in the corresponding entry in the {@code ids} array,
     * but only if the {@code indexFilter} returns {@code true} for the given index.
     *
     * @param names The array of token names we potentially want to create new ids for.
     * @param ids The array into which we still store the id we create for the various token names.
     * @param indexFilter A filter for the array indexes for which a token needs an id.
     * @throws KernelException If the inner transaction used to allocate the tokens encountered a problem.
     */
    default void createTokens( String[] names, int[] ids, IntPredicate indexFilter ) throws KernelException
    {
        for ( int i = 0; i < ids.length; i++ )
        {
            if ( indexFilter.test( i ) )
            {
                ids[i] = createToken( names[i] );
            }
        }
    }
}
