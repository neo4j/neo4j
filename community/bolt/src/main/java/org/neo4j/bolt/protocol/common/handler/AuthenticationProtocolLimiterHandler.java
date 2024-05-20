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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.Stack;
import org.neo4j.bolt.protocol.error.ClientRequestComplexityExceeded;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;

public class AuthenticationProtocolLimiterHandler extends SimpleChannelInboundHandler<ByteBuf> {

    public static final long SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(AuthenticationProtocolLimiterHandler.class);

    // TODO: Internal configuration parameters
    private final int maxElements;
    private final int maxMessageDepth;

    private final Stack<DecoderLevel> levels = new Stack<>();

    public AuthenticationProtocolLimiterHandler(int maxElements, int maxMessageDepth) {
        this.maxElements = maxElements;
        this.maxMessageDepth = maxMessageDepth;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        if (!msg.isReadable()) {
            ctx.fireChannelRead(msg.retain());
            return;
        }

        msg.markReaderIndex();
        var buffer = PackstreamBuf.wrap(msg);

        var rootEncountered = false;
        while (msg.isReadable()) {
            var type = buffer.peekType();

            if (this.levels.isEmpty()) {
                if (rootEncountered) {
                    throw new PackstreamReaderException("Encountered illegal secondary root element within message");
                }

                rootEncountered = true;

                if (type != Type.STRUCT) {
                    throw new PackstreamReaderException("Encountered illegal root element: Expected struct");
                }
            }

            switch (type) {
                case LIST, MAP -> this.pushLevel(type, buffer.readLengthPrefixMarker(type));
                case STRUCT -> this.pushLevel(type, buffer.readStructHeader().length());
                default -> {
                    buffer.skip(type);

                    if (this.flipMapKey(type)) {
                        continue;
                    }

                    this.popLevel();
                }
            }
        }

        msg.resetReaderIndex();
        ctx.fireChannelRead(msg.retain());
    }

    private boolean flipMapKey(Type type) throws PackstreamReaderException {
        if (this.levels.isEmpty()) {
            return false;
        }

        // maps contain two values per element (a string key and a value) thus requiring
        // us to keep track of when to pop
        var currentLevel = this.levels.peek();
        if (currentLevel.type == Type.MAP) {
            currentLevel.expectingKey = !currentLevel.expectingKey;

            if (!currentLevel.expectingKey) {
                if (type != Type.STRING) {
                    throw new PackstreamReaderException("Encountered illegal map element: Expected string key");
                }

                return true;
            }
        }

        return false;
    }

    private void pushLevel(Type type, long remainingElements) throws PackstreamReaderException {
        if (this.levels.size() + 1 > maxMessageDepth) {
            throw new ClientRequestComplexityExceeded(
                    "Message has exceeded maximum permitted complexity of " + maxMessageDepth + " levels");
        }
        if (remainingElements > maxElements) {
            throw new ClientRequestComplexityExceeded(
                    "Message has exceeded maximum permitted complexity of " + maxElements + " elements");
        }

        this.flipMapKey(type);

        if (remainingElements == 0) {
            this.popLevel();
            return;
        }

        this.levels.push(new DecoderLevel(type, remainingElements));
    }

    private void popLevel() {
        while (!this.levels.isEmpty()) {
            var currentLevel = this.levels.peek();
            if (--currentLevel.remainingElements > 0) {
                return;
            }

            this.levels.pop();
        }
    }

    private static final class DecoderLevel {

        private final Type type;
        private long remainingElements;
        private boolean expectingKey = true;

        public DecoderLevel(Type type, long remainingElements) {
            this.type = type;
            this.remainingElements = remainingElements;
        }
    }
}
