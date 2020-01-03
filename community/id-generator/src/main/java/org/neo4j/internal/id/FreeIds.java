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
package org.neo4j.internal.id;

import java.io.IOException;
import java.util.function.LongConsumer;

/**
 * Accessor/stream of free ids, for rebuild purposes.
 */
public interface FreeIds
{
    /**
     * Convenient instance for telling the {@link IdGenerator} that there are no free ids to rebuild its id generator from.
     * {@code -1} simply means that the highest id return was -1 so that "high id" becomes 0 (highest id + 1).
     */
    FreeIds NO_FREE_IDS = ignore -> -1;

    /**
     * @param visitor consumer of the free ids.
     * @return the highest id visited.
     * @throws IOException on I/O error.
     */
    long accept( LongConsumer visitor ) throws IOException;
}
