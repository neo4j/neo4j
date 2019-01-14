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

import java.util.List;

import org.neo4j.storageengine.api.Token;

public interface TokenHolder<TOKEN extends Token>
{
    int NO_ID = -1;

    void setInitialTokens( List<TOKEN> tokens ) throws NonUniqueTokenException;

    void addToken( TOKEN token ) throws NonUniqueTokenException;

    int getOrCreateId( String name );

    TOKEN getTokenById( int id ) throws TokenNotFoundException;

    TOKEN getTokenByIdOrNull( int id );

    /** Returns the id, or {@link #NO_ID} if no token with this name exists. */
    int getIdByName( String name );

    Iterable<TOKEN> getAllTokens();

    int size();
}
