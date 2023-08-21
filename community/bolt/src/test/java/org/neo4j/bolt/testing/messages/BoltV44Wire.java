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
package org.neo4j.bolt.testing.messages;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.protocol.io.writer.DefaultStructWriter;
import org.neo4j.bolt.protocol.io.writer.LegacyStructWriter;
import org.neo4j.bolt.protocol.v44.BoltProtocolV44;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public final class BoltV44Wire extends AbstractBoltWire {

    public BoltV44Wire() {
        super(BoltProtocolV44.VERSION);
    }

    @Override
    public boolean supportsLogonMessage() {
        return false;
    }

    @Override
    protected void configurePipeline() {
        this.pipeline.addLast(DefaultStructWriter.getInstance());
        this.pipeline.addFirst(LegacyStructWriter.getInstance());
    }

    @Override
    public String getUserAgent() {
        return "BoltWire/4.4";
    }

    @Override
    public void nodeValue(PackstreamBuf buf, String elementId, int id, List<String> labels) {
        buf.writeStructHeader(new StructHeader(3, StructType.NODE.getTag()))
                .writeInt(id)
                .writeList(labels, PackstreamBuf::writeString)
                .writeMapHeader(2)
                .writeString("theAnswer")
                .writeInt(42)
                .writeString("one_does_not_simply")
                .writeString("break_decoding");
    }

    @Override
    public void relationshipValue(
            PackstreamBuf buf,
            String elementId,
            int id,
            String startElementId,
            int startId,
            String endElementId,
            int endId,
            String type) {
        buf.writeStructHeader(new StructHeader(5, StructType.RELATIONSHIP.getTag()))
                .writeInt(id)
                .writeInt(startId)
                .writeInt(endId)
                .writeString(type)
                .writeMapHeader(2)
                .writeString("the_answer")
                .writeInt(42)
                .writeString("one_does_not_simply")
                .writeString("break_decoding");
    }

    @Override
    public void unboundRelationshipValue(PackstreamBuf buf, String elementId, int id, String type) {
        buf.writeStructHeader(new StructHeader(5, StructType.RELATIONSHIP.getTag()))
                .writeInt(id)
                .writeString(type)
                .writeMapHeader(2)
                .writeString("the_answer")
                .writeInt(42)
                .writeString("one_does_not_simply")
                .writeString("break_decoding");
    }

    @Override
    public ByteBuf logon(Map<String, Object> authToken) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf logoff() {
        throw new UnsupportedOperationException();
    }
}
