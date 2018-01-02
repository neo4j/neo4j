/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.io.fs.StoreChannel;

/**
 * Provides information and functionality for bridging log file boundaries.
 */
public interface LogVersionBridge
{
    /**
     * Provides the next channel, given the current channel and version.
     * Returning the same value as was passed in means that no bridging was needed or that the end was reached.
     *
     * @param channel {@link StoreChannel} to advance from.
     * @return the next {@link StoreChannel} having advanced on from the given channel, or {@code channel}
     * if no bridging needed or end was reached.
     * @throws IOException on error opening next version channel.
     */
    LogVersionedStoreChannel next( LogVersionedStoreChannel channel ) throws IOException;

    LogVersionBridge NO_MORE_CHANNELS = new LogVersionBridge()
    {
        @Override
        public LogVersionedStoreChannel next( LogVersionedStoreChannel channel ) throws IOException
        {
            return channel;
        }
    };
}
