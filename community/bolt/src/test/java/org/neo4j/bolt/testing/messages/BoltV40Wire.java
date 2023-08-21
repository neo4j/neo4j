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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.protocol.io.writer.LegacyStructWriter;
import org.neo4j.bolt.protocol.v40.BoltProtocolV40;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public class BoltV40Wire extends AbstractBoltWire {

    public BoltV40Wire() {
        super(BoltProtocolV40.VERSION);
    }

    protected BoltV40Wire(ProtocolVersion version, Feature... implicitFeatures) {
        super(version, implicitFeatures);
    }

    @Override
    public boolean supportsLogonMessage() {
        return false;
    }

    @Override
    protected void configurePipeline() {
        this.pipeline.addLast(LegacyStructWriter.getInstance());

        super.configurePipeline();
    }

    @Override
    public String getUserAgent() {
        return "BoltWire/4.0";
    }

    @Override
    public ByteBuf route(RoutingContext context, Collection<String> bookmarks, String db, String impersonatedUser) {
        var buf = PackstreamBuf.allocUnpooled();

        Map<String, String> routingParams;
        if (context != null) {
            routingParams = context.getParameters();
        } else {
            routingParams = Collections.emptyMap();
        }

        Collection<String> bookmarkStrings = bookmarks;
        if (bookmarkStrings == null) {
            bookmarkStrings = Collections.emptyList();
        }

        buf.writeStructHeader(new StructHeader(3, MESSAGE_TAG_ROUTE))
                .writeMap(routingParams, PackstreamBuf::writeString)
                .writeList(bookmarkStrings, PackstreamBuf::writeString);

        if (db == null) {
            buf.writeNull();
        } else {
            buf.writeString(db);
        }

        return buf.getTarget();
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
    public ByteBuf logoff() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf logon(Map<String, Object> authToken) {
        throw new UnsupportedOperationException();
    }
}
