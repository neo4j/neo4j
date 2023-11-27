/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.token;

import java.util.function.IntPredicate;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;

/**
 * When the database is marked as read-only, then no tokens can be created.
 */
public class ReadOnlyTokenCreator implements TokenCreator {
    public static final TokenCreator READ_ONLY = new ReadOnlyTokenCreator();

    private ReadOnlyTokenCreator() {}

    @Override
    public int createToken(String name, boolean internal) throws ReadOnlyDbException {
        throw new ReadOnlyDbException();
    }

    @Override
    public void createTokens(String[] names, int[] ids, boolean internal, IntPredicate filter)
            throws ReadOnlyDbException {
        throw new ReadOnlyDbException();
    }
}
