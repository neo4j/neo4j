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
package org.neo4j.bolt.protocol.common.connector.connection.listener;

import io.netty.channel.ChannelPipeline;
import java.time.Duration;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.handler.AuthenticationTimeoutHandler;
import org.neo4j.bolt.protocol.common.handler.HouseKeeperHandler;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;

/**
 * Handles the addition and removal of {@link AuthenticationTimeoutHandler} to/from the channel pipeline.
 */
public class AuthenticationTimeoutConnectionListener implements ConnectionListener {
    public static final long SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(AuthenticationTimeoutConnectionListener.class);

    private final Connection connection;
    private final Duration timeout;
    private final InternalLog log;

    private volatile AuthenticationTimeoutHandler handler;

    public AuthenticationTimeoutConnectionListener(
            Connection connection, Duration timeout, InternalLogProvider logging) {
        this.connection = connection;
        this.timeout = timeout;
        this.log = logging.getLog(AuthenticationTimeoutConnectionListener.class);
    }

    @Override
    public void onListenerRemoved() {
        this.connection.memoryTracker().releaseHeap(SHALLOW_SIZE);
    }

    @Override
    public void onNetworkPipelineInitialized(ChannelPipeline pipeline) {
        log.debug("[%s] Installing authentication timeout handler", this.connection.id());
        connection.memoryTracker().allocateHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);
        handler = new AuthenticationTimeoutHandler(timeout);

        pipeline.addLast(handler);
    }

    @Override
    public void onRequestReceived(RequestMessage message) {
        if (handler != null) {
            log.debug("[%s] Received request during authentication phase", this.connection.id());
            handler.setRequestReceived(true);
        }
    }

    @Override
    public void onLogon(LoginContext ctx) {
        log.debug("[%s] Removing authentication timeout handler", this.connection.id());

        if (handler != null) {
            this.connection.channel().pipeline().remove(handler);
            this.handler = null;
        }
    }

    @Override
    public void onLogoff() {
        log.debug("[%s] Re-adding authentication timeout handler", this.connection.id());
        connection.memoryTracker().allocateHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);

        handler = new AuthenticationTimeoutHandler(timeout);
        connection
                .channel()
                .pipeline()
                .addBefore(HouseKeeperHandler.HANDLER_NAME, "authenticationTimeoutHandler", handler);
    }
}
