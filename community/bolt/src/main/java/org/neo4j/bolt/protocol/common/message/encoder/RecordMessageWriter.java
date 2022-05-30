/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.message.encoder;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import java.io.IOException;
import java.util.function.Consumer;
import org.neo4j.bolt.protocol.common.message.result.BoltResult;
import org.neo4j.bolt.protocol.common.message.result.ResponseHandler;
import org.neo4j.bolt.protocol.common.signal.MessageSignal;
import org.neo4j.bolt.protocol.io.BoltValueWriter;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.AnyValue;

public class RecordMessageWriter implements BoltResult.RecordConsumer {
    public static final short RECORD_TAG = 0x71;

    private final Channel channel;
    private final ResponseHandler parent;

    public RecordMessageWriter(Channel channel, ResponseHandler parent) {
        this.channel = channel;
        this.parent = parent;
    }

    private void write(Consumer<PackstreamBuf> consumer) throws IOException {
        var buf = this.channel.alloc().buffer();
        consumer.accept(PackstreamBuf.wrap(buf));

        this.channel.write(buf).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    }

    @Override
    public void beginRecord(int numberOfFields) throws IOException {
        this.write(buf -> buf.writeStructHeader(new StructHeader(1, RECORD_TAG)).writeListHeader(numberOfFields));
    }

    @Override
    public void consumeField(AnyValue value) throws IOException {
        this.write(b -> value.writeTo(new BoltValueWriter(b)));
    }

    @Override
    public void endRecord() throws IOException {
        this.channel.write(MessageSignal.END).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    }

    @Override
    public void onError() throws IOException {
        this.channel.write(MessageSignal.RESET).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    }

    @Override
    public void addMetadata(String key, AnyValue value) {
        this.parent.onMetadata(key, value);
    }
}
