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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.io.IOException;
import java.net.SocketAddress;

public class LocalConnection extends AbstractTransportConnection {

    private static final Factory factory = new Factory();
    private final SocketAddress socketAddress;
    private Channel channel;
    private EventLoopGroup eventLoopGroup;
    private final ByteBuf outputBytes = ByteBufAllocator.DEFAULT.buffer();

    public LocalConnection(SocketAddress socketAddress) {
        this.socketAddress = socketAddress;
    }

    public static TransportConnection.Factory factory() {
        return factory;
    }

    @Override
    public TransportConnection connect() throws IOException {
        var cb = new Bootstrap();
        eventLoopGroup = new NioEventLoopGroup(1);
        cb.group(eventLoopGroup).channel(LocalChannel.class).handler(new ChannelInitializer<LocalChannel>() {
            @Override
            public void initChannel(LocalChannel ch) throws Exception {
                ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
                ch.pipeline().addLast(new ByteBufAccumulatingHandler(outputBytes));
            }
        });

        // Start the client.
        try {
            channel = cb.connect(socketAddress).sync().channel();
        } catch (InterruptedException e) {
            throw new IOException("Unable to establish local connection" + e);
        }

        return this;
    }

    @Override
    public TransportConnection disconnect() throws IOException {
        channel.close();
        try {
            eventLoopGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            throw new IOException("Error whilst disconnecting from local channel" + e);
        }

        return this;
    }

    @Override
    public TransportConnection sendRaw(ByteBuf buf) throws IOException {
        channel.writeAndFlush(buf);
        return this;
    }

    @Override
    public ByteBuf receive(int length) throws IOException, InterruptedException {
        while (true) {
            if (outputBytes.readableBytes() >= length) {
                return outputBytes.readBytes(length);
            }
            // allow some time for the buffer to fill if there are insufficient bytes.
            Thread.sleep(50);
        }
    }

    @Override
    public boolean isClosed() throws InterruptedException {
        return !channel.isOpen();
    }

    private static class ByteBufAccumulatingHandler extends ChannelInboundHandlerAdapter {

        private ByteBuf outputBytes;

        protected ByteBufAccumulatingHandler(ByteBuf outputBytes) {
            this.outputBytes = outputBytes;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            outputBytes = outputBytes.writeBytes((ByteBuf) msg);
        }
    }

    private static class Factory implements TransportConnection.Factory {

        @Override
        public TransportConnection create(SocketAddress address) {
            return new LocalConnection(address);
        }

        @Override
        public String toString() {
            return "Local Channel";
        }
    }
}
