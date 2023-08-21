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
package org.neo4j.bolt.transport;

import io.netty.channel.Channel;
import org.neo4j.memory.MemoryTracker;

public interface TransportThrottle {

    /**
     * Installs the throttle to the given channel.
     *
     * @param channel the netty channel to which this throttle should be installed
     * @param memoryTracker the pool from which memory is to be allocated
     */
    void install(Channel channel, MemoryTracker memoryTracker);

    /**
     * Apply throttling logic for the given channel..
     *
     * @param channel the netty channel to which this throttling logic should be applied
     * @throws TransportThrottleException when throttle decides this connection should be halted
     */
    void acquire(Channel channel) throws TransportThrottleException;

    /**
     * Release throttling for the given channel (if applied)..
     *
     * @param channel the netty channel from which this throttling logic should be released
     */
    void release(Channel channel);

    /**
     * Uninstalls the throttle from the given channel.
     *
     * @param channel the netty channel from which this throttle should be uninstalled
     */
    void uninstall(Channel channel);
}
