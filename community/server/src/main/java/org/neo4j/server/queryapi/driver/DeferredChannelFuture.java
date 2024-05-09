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
package org.neo4j.server.queryapi.driver;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.util.Futures;

class DeferredChannelFuture implements ChannelFuture {
    private final Logger logger;

    @SuppressWarnings("rawtypes")
    private final CompletableFuture<GenericFutureListener> listenerFuture = new CompletableFuture<>();

    @SuppressWarnings("unchecked")
    public DeferredChannelFuture(CompletionStage<ChannelFuture> channelFutureCompletionStage, Logging logging) {
        this.logger = logging.getLog(getClass());

        listenerFuture.thenCompose(ignored -> channelFutureCompletionStage).whenComplete((channelFuture, throwable) -> {
            var listener = listenerFuture.join();
            if (throwable != null) {
                throwable = Futures.completionExceptionCause(throwable);
                try {
                    listener.operationComplete(new DeferredChannelFuture.FailedChannelFuture(throwable));
                } catch (Throwable e) {
                    logger.error("An error occurred while notifying listener.", e);
                }
            } else {
                channelFuture.addListener(listener);
            }
        });
    }

    @Override
    public Channel channel() {
        return null;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public boolean isCancellable() {
        return false;
    }

    @Override
    public Throwable cause() {
        return null;
    }

    @Override
    public ChannelFuture addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        listenerFuture.complete(listener);
        return this;
    }

    @SafeVarargs
    @Override
    public final ChannelFuture addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        return null;
    }

    @Override
    public ChannelFuture removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        return null;
    }

    @SafeVarargs
    @Override
    public final ChannelFuture removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        return null;
    }

    @Override
    public ChannelFuture sync() {
        return null;
    }

    @Override
    public ChannelFuture syncUninterruptibly() {
        return null;
    }

    @Override
    public ChannelFuture await() {
        return null;
    }

    @Override
    public ChannelFuture awaitUninterruptibly() {
        return null;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) {
        return false;
    }

    @Override
    public boolean await(long timeoutMillis) {
        return false;
    }

    @Override
    public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
        return false;
    }

    @Override
    public boolean awaitUninterruptibly(long timeoutMillis) {
        return false;
    }

    @Override
    public Void getNow() {
        return null;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public Void get() {
        return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) {
        return null;
    }

    @Override
    public boolean isVoid() {
        return false;
    }

    private record FailedChannelFuture(Throwable throwable) implements ChannelFuture {

        @Override
        public Channel channel() {
            return null;
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public boolean isCancellable() {
            return false;
        }

        @Override
        public Throwable cause() {
            return throwable;
        }

        @Override
        public ChannelFuture addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
            return null;
        }

        @SafeVarargs
        @Override
        public final ChannelFuture addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
            return null;
        }

        @Override
        public ChannelFuture removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
            return null;
        }

        @SafeVarargs
        @Override
        public final ChannelFuture removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
            return null;
        }

        @Override
        public ChannelFuture sync() {
            return null;
        }

        @Override
        public ChannelFuture syncUninterruptibly() {
            return null;
        }

        @Override
        public ChannelFuture await() {
            return null;
        }

        @Override
        public ChannelFuture awaitUninterruptibly() {
            return null;
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) {
            return false;
        }

        @Override
        public boolean await(long timeoutMillis) {
            return false;
        }

        @Override
        public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
            return false;
        }

        @Override
        public boolean awaitUninterruptibly(long timeoutMillis) {
            return false;
        }

        @Override
        public Void getNow() {
            return null;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Void get() {
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) {
            return null;
        }

        @Override
        public boolean isVoid() {
            return false;
        }
    }
}
