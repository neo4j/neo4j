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
package org.neo4j.kernel.api.index;

/**
 * Progressor over the entities of a token index.
 */
public interface TokenIndexProgressor extends IndexProgressor {
    /**
     * Position the progressor so that a subsequent {@link #next()} moves it to the entity with the given id if it
     * exists, or otherwise to the next entity in the progressor's order.
     */
    void skipUntil(long id);

    TokenIndexProgressor EMPTY = new TokenIndexProgressor() {
        @Override
        public boolean next() {
            return false;
        }

        @Override
        public void skipUntil(long id) {}

        @Override
        public void close() {}
    };
}
