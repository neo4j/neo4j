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
package org.neo4j.bolt.protocol.common.fsm.response;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.ReferenceCountUtil;
import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.io.pipeline.PipelineContext;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.signal.FrameSignal;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.AnyValue;

public class NetworkRecordHandler implements RecordHandler, Closeable {
    public static final short RECORD_TAG = 0x71;

    private final Connection connection;
    private final int numberOfFields;
    private final int bufferSize;
    private final int flushThreshold;

    private PackstreamBuf buffer;
    private PipelineContext writerContext;
    private final List<ByteBuf> pendingMessages = new LinkedList<>();

    public NetworkRecordHandler(Connection connection, int numberOfFields, int bufferSize, int flushThreshold) {
        this.connection = connection;
        this.numberOfFields = numberOfFields;
        this.bufferSize = bufferSize;
        this.flushThreshold = flushThreshold;
    }

    @Override
    public void onBegin() {
        // if no buffer has been allocated yet (e.g. because a prior record was flushed or this is
        // the first record), we'll allocate a new instance
        if (this.buffer == null) {
            this.buffer = PackstreamBuf.wrap(connection.channel().alloc().buffer(bufferSize));
            this.writerContext = connection.writerContext(this.buffer);
        }

        this.buffer.writeStructHeader(new StructHeader(1, RECORD_TAG)).writeListHeader(this.numberOfFields);
    }

    @Override
    public void onField(AnyValue value) {
        this.writerContext.writeValue(value);
    }

    @Override
    public void onCompleted() {
        var buffer = this.buffer.getTarget();

        this.pendingMessages.add(buffer.readRetainedSlice(buffer.readableBytes()));
        buffer.markWriterIndex();

        if (this.flushThreshold == 0 || this.buffer.getTarget().writerIndex() >= this.flushThreshold) {
            // if there is no flush threshold, or we have exceeded the configured amount, we'll flush
            // the record into the network pipeline - this is necessary as writes and especially
            // flushing can be somewhat costly
            this.flush();
        }
    }

    @Override
    public void onFailure() {
        this.close();
    }

    private void writePending() {
        // ensure that we release our copy of the buffer as all slices are retained separately
        // resulting in them not being released by netty upon write completion
        ReferenceCountUtil.release(this.buffer);
        this.buffer = null;

        // pass each message separately in order to retain chunk encoding capabilities within the
        // pipeline
        this.pendingMessages.forEach(message -> {
            this.connection.write(message).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            this.connection.write(FrameSignal.MESSAGE_END).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        });
        this.pendingMessages.clear();
    }

    private void flush() {
        this.writePending();
        this.connection.flush();

        // as a result of this asynchronous call, we've lost control over the buffer and will thus
        // need to allocate a new buffer to work with from now on
        this.buffer = null;
        this.writerContext = null;
    }

    @Override
    public void close() {
        if (this.buffer == null) {
            return;
        }

        // write any pending records to the pipeline without explicitly flushing - this will always
        // be followed by a result message and a flush call
        this.writePending();
    }
}
