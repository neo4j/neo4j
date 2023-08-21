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
package org.neo4j.bolt.protocol.common.handler;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.AsciiString;
import java.util.Date;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.server.config.AuthConfigProvider;

/**
 * Handler that responds with discovery configuration with a non-websocket upgrade request. Otherwise,
 * it passes the request to the next handler and removes itself.
 */
public class DiscoveryResponseHandler extends ChannelInboundHandlerAdapter {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(DiscoveryResponseHandler.class);

    private final AuthConfigProvider authConfigProvider;

    public DiscoveryResponseHandler(AuthConfigProvider authConfigProvider) {
        this.authConfigProvider = authConfigProvider;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        FullHttpMessage httpMessage = (FullHttpMessage) msg;
        if (!isWebsocketUpgrade(httpMessage.headers())) {
            var discoveryResponse = new DefaultFullHttpResponse(
                    httpMessage.protocolVersion(),
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(authConfigProvider.getRepresentationAsBytes()));
            addHeaders(discoveryResponse);
            ctx.writeAndFlush(discoveryResponse).addListener(ChannelFutureListener.CLOSE);
        } else {
            ctx.pipeline().remove(this);
            ctx.fireChannelRead(msg);
        }
    }

    private void addHeaders(FullHttpMessage discoveryResponse) {
        discoveryResponse.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        discoveryResponse.headers().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, AsciiString.cached("*"));
        discoveryResponse.headers().add(HttpHeaderNames.VARY, AsciiString.cached("Accept"));
        discoveryResponse
                .headers()
                .add(HttpHeaderNames.CONTENT_LENGTH, discoveryResponse.content().readableBytes());
        discoveryResponse.headers().add(HttpHeaderNames.DATE, new Date());
        HttpUtil.setContentLength(discoveryResponse, discoveryResponse.content().readableBytes());
    }

    private static boolean isWebsocketUpgrade(HttpHeaders headers) {
        return headers.contains(HttpHeaderNames.UPGRADE)
                && headers.containsValue(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE, true)
                && headers.contains(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true);
    }
}
