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

import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.decoder.MessageDecoder;
import org.neo4j.bolt.protocol.common.message.request.streaming.AbstractStreamingMessage;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.util.PackstreamConditions;
import org.neo4j.packstream.util.PackstreamConversions;
import org.neo4j.values.virtual.MapValue;

public abstract sealed class AbstractStreamingMessageDecoder<M extends AbstractStreamingMessage>
        implements MessageDecoder<M> permits DefaultDiscardMessageDecoder, DefaultPullMessageDecoder {

    private static final String FIELD_STREAM_LIMIT = "n";
    private static final String FIELD_QUERY_ID = "qid";

    private static final long STREAM_LIMIT_MINIMAL = 1;
    public static final long STREAM_LIMIT_UNLIMITED = -1;

    @Override
    public M read(Connection ctx, PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        PackstreamConditions.requireLength(header, 1);

        var valueReader = ctx.valueReader(buffer);

        MapValue meta;
        try {
            meta = valueReader.readMap();
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("meta", ex);
        }

        try {
            var n = this.readStreamLimit(meta);
            var statementId = this.readStatementId(meta);

            return this.createMessageInstance(n, statementId);
        } catch (PackstreamReaderException ex) {
            throw new IllegalStructArgumentException("meta", ex);
        }
    }

    protected abstract M createMessageInstance(long n, long statementId);

    protected long readStreamLimit(MapValue meta) throws PackstreamReaderException {
        var size = PackstreamConversions.asLongValue(FIELD_STREAM_LIMIT, meta.get(FIELD_STREAM_LIMIT));

        if (size != STREAM_LIMIT_UNLIMITED && size < STREAM_LIMIT_MINIMAL) {
            throw new IllegalStructArgumentException(
                    "n", String.format("Expecting size to be at least %s, but got: %s", STREAM_LIMIT_MINIMAL, size));
        }

        return size;
    }

    protected long readStatementId(MapValue meta) throws PackstreamReaderException {
        var statementId = PackstreamConversions.asNullableLongValue(FIELD_QUERY_ID, meta.get(FIELD_QUERY_ID));
        if (!statementId.isPresent()) {
            return -1;
        }

        return statementId.getAsLong();
    }
}
