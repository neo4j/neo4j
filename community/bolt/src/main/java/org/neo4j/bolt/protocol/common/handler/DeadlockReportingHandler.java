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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.net.SocketAddress;
import org.neo4j.bolt.protocol.common.connector.accounting.thread.ThreadAccountant;
import org.neo4j.memory.HeapEstimator;

public class DeadlockReportingHandler extends ChannelDuplexHandler {

    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(DeadlockReportingHandler.class);

    private final ThreadAccountant accountant;

    public DeadlockReportingHandler(ThreadAccountant accountant) {
        this.accountant = accountant;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        this.accountant.execute("channelRegistered", ctx::fireChannelRegistered);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        this.accountant.execute("channelUnregistered", ctx::fireChannelUnregistered);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.accountant.execute("channelActive", ctx::fireChannelActive);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.accountant.execute("channelInactive", ctx::fireChannelInactive);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        this.accountant.execute("channelRead", () -> {
            ctx.fireChannelRead(msg);
        });
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        this.accountant.execute("channelReadComplete", ctx::fireChannelReadComplete);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        this.accountant.execute("userEventTriggered", () -> ctx.fireUserEventTriggered(evt));
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        this.accountant.execute("channelWritabilityChanged", ctx::fireChannelWritabilityChanged);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        this.accountant.execute("exceptionCaught", () -> ctx.fireExceptionCaught(cause));
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        this.accountant.execute("flush", ctx::flush);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        this.accountant.execute("write", () -> ctx.write(msg, promise));
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        this.accountant.execute("read", () -> ctx.read());
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        this.accountant.execute("deregister", () -> ctx.deregister(promise));
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        this.accountant.execute("close", () -> ctx.close(promise));
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        this.accountant.execute("disconnect", () -> ctx.disconnect(promise));
    }

    @Override
    public void connect(
            ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise)
            throws Exception {
        this.accountant.execute("connect", () -> ctx.connect(remoteAddress, localAddress, promise));
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        this.accountant.execute("bind", () -> ctx.bind(localAddress, promise));
    }
}
