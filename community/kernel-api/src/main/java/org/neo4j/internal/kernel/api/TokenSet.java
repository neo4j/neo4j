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
package org.neo4j.internal.kernel.api;

import java.util.NoSuchElementException;

/**
 * Set of token ids.
 *
 * Modifications are not reflected in the TokenSet and there is no guaranteed
 * order.
 */
public interface TokenSet
{
    int numberOfTokens();

    int token( int offset );

    boolean contains( int token );

    long[] all();

    TokenSet NONE = new TokenSet()
    {
        private final long[] EMPTY = new long[0];

        @Override
        public int numberOfTokens()
        {
            return 0;
        }

        @Override
        public int token( int offset )
        {
            throw new NoSuchElementException();
        }

        @Override
        public boolean contains( int token )
        {
            return false;
        }

        @Override
        public long[] all()
        {
            return EMPTY;
        }
    };
}
