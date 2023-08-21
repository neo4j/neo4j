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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import org.neo4j.io.fs.StoreChannel;

/**
 * Provides information and functionality for bridging log file boundaries.
 */
@FunctionalInterface
public interface LogVersionBridge {
    LogVersionBridge NO_MORE_CHANNELS = (channel, raw) -> channel;

    /**
     * Provides the next channel, given the current channel and version.
     * Returning the same value as was passed in means that no bridging was needed or that the end was reached.
     *
     * @param channel {@link StoreChannel} to advance from.
     * @param raw flag to specify if raw channel should open. Raw channel will not gonna perform any calls to pre-load, offload file content from page cache
     * and potentially will not perform some other optimisations.
     * @return the next {@link StoreChannel} having advanced on from the given channel, or {@code channel}
     * if no bridging needed or end was reached.
     * @throws IOException on error opening next version channel.
     */
    LogVersionedStoreChannel next(LogVersionedStoreChannel channel, boolean raw) throws IOException;
}
