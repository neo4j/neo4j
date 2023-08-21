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
package org.neo4j.bolt.protocol.common.connector.transport;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.unix.ServerDomainSocketChannel;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.neo4j.annotations.service.Service;
import org.neo4j.service.PrioritizedService;
import org.neo4j.service.Services;

/**
 * Encapsulates a network transport in a system agnostic fashion.
 */
@Service
public interface ConnectorTransport extends PrioritizedService {

    /**
     * Retrieves a stream of available transport implementations within the application Class-Path.
     *
     * @return a stream of available transports.
     */
    static Stream<ConnectorTransport> listAvailable() {
        return Services.loadAll(ConnectorTransport.class).stream();
    }

    /**
     * Selects the most optimal available transport from a list of transport implementations.
     *
     * @return an optimal transport or an empty optional if none is available.
     */
    static Optional<ConnectorTransport> selectOptimal(Predicate<ConnectorTransport> filter) {
        return listAvailable()
                .filter(filter)
                .filter(ConnectorTransport::isAvailable)
                .min(Comparator.comparingInt(ConnectorTransport::getPriority));
    }

    /**
     * Retrieves a human-readable name with which this transport implementation is identified within the internal
     * application log.
     *
     * @return a human readable name.
     */
    String getName();

    @Override
    default int getPriority() {
        // zero indicates no preference over other implementations and shall act as a default
        return 0;
    }

    /**
     * Evaluates whether this transport is available within the current execution environment.
     *
     * @return true if available, false otherwise.
     */
    boolean isAvailable();

    /**
     * Evaluates whether this transport relies on native libraries in order to implement its logic.
     *
     * @return true if native libraries are used, false otherwise.
     */
    boolean isNative();

    /**
     * Creates a new event loop group for use with the channel implementations designated by this transport.
     *
     * @param threadCount the total number of threads within the new pool.
     * @param threadFactory a factory capable of creating threads for the new pool.
     * @return a compatible event loop group.
     */
    EventLoopGroup createEventLoopGroup(int threadCount, ThreadFactory threadFactory);

    default EventLoopGroup createEventLoopGroup(ThreadFactory threadFactory) {
        return this.createEventLoopGroup(0, threadFactory);
    }

    /**
     * Retrieves the channel implementation which is used to bind server sockets.
     *
     * @return a server socket implementation.
     */
    Class<? extends ServerSocketChannel> getSocketChannelType();

    /**
     * Retrieves the channel implementation which is used to bind domain server sockets.
     *
     * @return a server socket implementation or null if unsupported.
     */
    Class<? extends ServerDomainSocketChannel> getDomainSocketChannelType();

    /**
     * Retrieves the channel implementation which is used to bind local transport for intra-VM communication.
     *
     * @return a local server channel implementation.
     */
    default Class<? extends LocalServerChannel> getLocalChannelType() {
        return LocalServerChannel.class;
    }
}
