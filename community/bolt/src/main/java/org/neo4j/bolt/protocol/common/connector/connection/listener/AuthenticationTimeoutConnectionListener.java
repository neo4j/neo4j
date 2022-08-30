/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.protocol.common.connector.connection.listener;

import io.netty.channel.ChannelPipeline;
import java.time.Duration;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.handler.AuthenticationTimeoutHandler;
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

        this.connection.memoryTracker().allocateHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);

        var handler = new AuthenticationTimeoutHandler(this.timeout);
        this.handler = handler;

        pipeline.addLast(handler);
    }

    @Override
    public void onRequestReceived(RequestMessage message) {
        log.debug("[%s] Received request during authentication phase", this.connection.id());

        var handler = this.handler;
        if (handler != null) {
            handler.setRequestReceived(true);
        }
    }

    @Override
    public void onAuthenticated(LoginContext ctx) {
        log.debug("[%s] Removing authentication timeout handler", this.connection.id());

        var handler = this.handler;
        if (handler != null) {
            this.connection.channel().pipeline().remove(handler);
        }

        this.connection.removeListener(this);
    }
}
