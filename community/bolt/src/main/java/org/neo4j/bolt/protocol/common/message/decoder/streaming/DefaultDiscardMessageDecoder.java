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
package org.neo4j.bolt.protocol.common.message.decoder.streaming;

import org.neo4j.bolt.protocol.common.message.request.streaming.DiscardMessage;

public final class DefaultDiscardMessageDecoder extends AbstractStreamingMessageDecoder<DiscardMessage> {
    private static final DefaultDiscardMessageDecoder INSTANCE = new DefaultDiscardMessageDecoder();

    private DefaultDiscardMessageDecoder() {}

    public static DefaultDiscardMessageDecoder getInstance() {
        return INSTANCE;
    }

    @Override
    public short getTag() {
        return DiscardMessage.SIGNATURE;
    }

    @Override
    protected DiscardMessage createMessageInstance(long n, long statementId) {
        return new DiscardMessage(n, statementId);
    }
}
