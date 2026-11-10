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
package org.neo4j.bolt.testing.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import org.neo4j.bolt.testing.client.error.BoltTestClientStateException;
import org.neo4j.bolt.testing.client.handler.WebSocketHandler;

public sealed class WebSocketConnection extends SocketConnection permits SecureWebSocketConnection {

    private static final Factory factory = new Factory();

    public WebSocketConnection(InetSocketAddress address) {
        super(address);
    }

    public static BoltTestConnection.Factory factory() {
        return factory;
    }

    @Override
    protected ChannelPromise initializeChannel(Channel ch) {
        super.initializeChannel(ch);

        var uri = this.webSocketAddress();
        var handler = new WebSocketHandler(
                WebSocketClientHandshakerFactory.newHandshaker(uri, WebSocketVersion.V13, null, false, null));

        ch.pipeline()
                .addBefore(INBOUND_HANDLER_NAME, "httpClientCodec", new HttpClientCodec())
                .addBefore(INBOUND_HANDLER_NAME, "httpObjectAggregator", new HttpObjectAggregator(65_535))
                .addBefore(
                        INBOUND_HANDLER_NAME,
                        "webSocketClientCompressionHandler",
                        WebSocketClientCompressionHandler.INSTANCE)
                .addBefore(INBOUND_HANDLER_NAME, "webSocketHandler", handler);

        return handler.handshakePromise();
    }

    protected URI webSocketAddress() {
        try {
            return new URI("ws", null, this.address.getHostString(), this.address.getPort(), "/", null, null);
        } catch (URISyntaxException ex) {
            throw new BoltTestClientStateException("Failed to construct WebSocket address", ex);
        }
    }

    private static class Factory implements BoltTestConnection.Factory {

        @Override
        public BoltTestConnection create(SocketAddress address) {
            if (address instanceof InetSocketAddress inetSocketAddress) {
                return new WebSocketConnection(inetSocketAddress);
            }

            throw new IllegalArgumentException("Cannot initialize WebSocket connection with address of type "
                    + address.getClass().getSimpleName());
        }

        @Override
        public String toString() {
            return "Plain WebSocket";
        }
    }
}
