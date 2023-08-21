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
package org.neo4j.io.pagecache.impl;

import java.io.IOException;
import org.neo4j.io.fs.StoreChannel;

sealed interface BlockSwapper permits UnsafeBlockSwapper, FallbackBlockSwapper {
    /**
     * Reads from channel to specified location in memory
     */
    int swapIn(StoreChannel channel, long bufferAddress, long fileOffset, int bufferSize) throws IOException;

    /**
     * Writes to channel from specified location in memory
     */
    void swapOut(StoreChannel channel, long bufferAddress, long fileOffset, int bufferLength) throws IOException;
}
