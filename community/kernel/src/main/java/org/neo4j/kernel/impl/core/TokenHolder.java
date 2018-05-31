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
package org.neo4j.kernel.impl.core;

import java.util.List;

import org.neo4j.storageengine.api.Token;

public interface TokenHolder<TOKEN extends Token>
{
    int NO_ID = -1;

    void setInitialTokens( List<TOKEN> tokens ) throws NonUniqueTokenException;

    void addToken( TOKEN token ) throws NonUniqueTokenException;

    /**
     * Get the id of the token by the given name, or create a new id for the token if it does not have one already,
     * and then return that id.
     * <p>
     * This method is thread-safe, and will ensure that distinct tokens will not have multiple ids allocated for them.
     *
     * @param name The name of the token to get the id for.
     * @return The (possibly newly created) id of the given token.
     */
    int getOrCreateId( String name );

    /**
     * Resolve the ids of the given token {@code names} into the array for {@code ids}.
     * <p>
     * Any tokens that don't already have an id will have one created for it.
     */
    void getOrCreateIds( String[] names, int[] ids );

    TOKEN getTokenById( int id ) throws TokenNotFoundException;

    TOKEN getTokenByIdOrNull( int id );

    /**
     * Returns the id, or {@link #NO_ID} if no token with this name exists.
     */
    int getIdByName( String name );

    /**
     * Resolve the ids of the given token {@code names} into the array for {@code ids}.
     * <p>
     * Any tokens that don't already have an id will not be resolved, and the corrosponding entry in the {@code ids}
     * array will be left untouched. If you wish for those unresolved id entries to end up with the {@link #NO_ID}
     * value, you must first fill the array with that value before calling this method.
     *
     * @return {@code true} if some of the token names could not be resolved, {@code false} otherwise.
     */
    boolean getIdsByNames( String[] names, int[] ids );

    Iterable<TOKEN> getAllTokens();

    int size();
}
