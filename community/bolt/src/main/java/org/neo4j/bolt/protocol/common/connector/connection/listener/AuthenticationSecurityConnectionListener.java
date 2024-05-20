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
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.handler.AuthenticationProtocolLimiterHandler;
import org.neo4j.bolt.protocol.common.handler.AuthenticationTimeoutHandler;
import org.neo4j.bolt.protocol.common.handler.HouseKeeperHandler;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.packstream.codec.transport.ChunkFrameDecoder;

/**
 * Handles the addition and removal of {@link AuthenticationTimeoutHandler} to/from the channel
 * pipeline.
 */
public class AuthenticationSecurityConnectionListener implements ConnectionListener {

    public static final long SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(AuthenticationSecurityConnectionListener.class);

    private final Connection connection;
    private final Duration timeout;
    private final InternalLog log;

    private volatile AuthenticationTimeoutHandler timeoutHandler;
    private volatile AuthenticationProtocolLimiterHandler protocolLimiterHandler;

    public AuthenticationSecurityConnectionListener(
            Connection connection, Duration timeout, InternalLogProvider logging) {
        this.connection = connection;
        this.timeout = timeout;
        this.log = logging.getLog(AuthenticationSecurityConnectionListener.class);
    }

    @Override
    public void onListenerRemoved() {
        this.connection.memoryTracker().releaseHeap(SHALLOW_SIZE);
    }

    @Override
    public void onNetworkPipelineInitialized(ChannelPipeline pipeline) {
        log.debug("[%s] Installing authentication timeout handler", this.connection.id());

        connection.memoryTracker().allocateHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);
        timeoutHandler = new AuthenticationTimeoutHandler(timeout);

        pipeline.addLast(timeoutHandler);
    }

    @Override
    public void onProtocolSelected(BoltProtocol protocol) {
        this.installStructureLimitHandler();
    }

    @Override
    public void onRequestReceived(RequestMessage message) {
        if (timeoutHandler != null) {
            log.debug("[%s] Received request during authentication phase", this.connection.id());
            timeoutHandler.setRequestReceived(true);
        }
    }

    @Override
    public void onLogon(LoginContext ctx) {
        log.debug("[%s] Removing authentication timeout handler", this.connection.id());

        if (timeoutHandler != null) {
            this.connection.channel().pipeline().remove(timeoutHandler);
            this.timeoutHandler = null;

            if (this.protocolLimiterHandler != null) {
                this.connection.channel().pipeline().remove(protocolLimiterHandler);

                this.protocolLimiterHandler = null;
            }
        }
    }

    @Override
    public void onLogoff() {
        log.debug("[%s] Re-adding authentication timeout handler", this.connection.id());
        connection.memoryTracker().allocateHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);

        timeoutHandler = new AuthenticationTimeoutHandler(timeout);

        connection
                .channel()
                .pipeline()
                .addBefore(HouseKeeperHandler.HANDLER_NAME, "authenticationTimeoutHandler", timeoutHandler);

        this.installStructureLimitHandler();
    }

    private void installStructureLimitHandler() {
        var config = this.connection.connector().configuration();
        var structureElementLimit = config.maxAuthenticationStructureElements();
        var structureDepthLimit = config.maxAuthenticationStructureDepth();

        if (structureElementLimit == 0 && structureDepthLimit == 0) {
            this.log.debug("[%s] Authentication structure limit is disabled", this.connection.id());
            return;
        }

        this.log.debug(
                "[%s] Imposing authentication structure limits of %d elements with a maximum depth of %d",
                this.connection.id(), structureElementLimit, structureDepthLimit);

        connection.memoryTracker().allocateHeap(AuthenticationProtocolLimiterHandler.SHALLOW_SIZE);
        protocolLimiterHandler = new AuthenticationProtocolLimiterHandler(structureElementLimit, structureDepthLimit);

        this.connection
                .channel()
                .pipeline()
                .addAfter(ChunkFrameDecoder.NAME, "protocolLimiterHandler", protocolLimiterHandler);
    }
}
