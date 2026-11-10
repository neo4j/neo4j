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
package org.neo4j.bolt.testing.client.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.neo4j.bolt.testing.client.error.BoltTestClientConnectionTimeoutException;

public class TestChannelInitializer extends ChannelInitializer<Channel> {
    private final Function<Channel, ChannelPromise> initializer;

    private ChannelPromise initializerFuture;

    public TestChannelInitializer(Function<Channel, ChannelPromise> initializer) {
        this.initializer = initializer;
    }

    public void awaitInitialization() throws InterruptedException {
        var future = this.initializerFuture;
        if (future == null) {
            throw new IllegalStateException("Channel has not been initialized yet");
        }

        if (!future.await(30, TimeUnit.SECONDS)) {
            throw new BoltTestClientConnectionTimeoutException(
                    "Failed to establish connection: Timed out while waiting for channel initializers");
        }
    }

    @Override
    protected void initChannel(Channel ch) {
        this.initializerFuture = ch.newPromise();

        this.initializer.apply(ch).addListener(f -> {
            if (this.initializerFuture.isDone()) {
                return;
            }

            if (f.isSuccess()) {
                this.initializerFuture.setSuccess();
            } else {
                this.initializerFuture.setFailure(f.cause());
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (!this.initializerFuture.isDone()) {
            this.initializerFuture.setFailure(cause);
        }

        super.exceptionCaught(ctx, cause);
    }
}
