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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.protocol.io.pipeline.WriterPipeline;
import org.neo4j.bolt.protocol.io.writer.DefaultStructWriter;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public abstract class AbstractBoltWire implements BoltWire {

    public static final short MESSAGE_TAG_BEGIN = (short) 0x11;
    public static final short MESSAGE_TAG_DISCARD = (short) 0x2F;
    public static final short MESSAGE_TAG_PULL = (short) 0x3F;
    public static final short MESSAGE_TAG_HELLO = (short) 0x01;
    public static final short MESSAGE_TAG_RUN = (short) 0x10;
    public static final short MESSAGE_TAG_ROLLBACK = (short) 0x13;
    public static final short MESSAGE_TAG_COMMIT = (short) 0x12;
    public static final short MESSAGE_TAG_RESET = (short) 0x0F;
    public static final short MESSAGE_TAG_GOODBYE = (short) 0x02;
    public static final short MESSAGE_TAG_ROUTE = (short) 0x66;
    public static final short MESSAGE_TAG_LOGON = (short) 0x6A;
    public static final short MESSAGE_TAG_LOGOFF = (short) 0x6B;

    protected final ProtocolVersion version;
    protected final Connection connection;
    protected WriterPipeline pipeline;
    protected final Set<Feature> implicitFeatures;
    protected final Set<Feature> features = new HashSet<>();

    protected AbstractBoltWire(ProtocolVersion version, Feature... implicitFeatures) {
        this.version = version;
        this.implicitFeatures = new HashSet<>(List.of(implicitFeatures));

        this.connection = ConnectionMockFactory.newInstance();

        Mockito.doAnswer(invocation -> this.pipeline.forBuffer(invocation.getArgument(0)))
                .when(connection)
                .writerContext(Mockito.any());

        this.initializePipeline();
    }

    protected void initializePipeline() {
        this.pipeline = new WriterPipeline(this.connection);

        this.configurePipeline();
    }

    protected void configurePipeline() {
        this.pipeline.addLast(DefaultStructWriter.getInstance());
    }

    @Override
    public WriterPipeline getPipeline() {
        return pipeline;
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return this.version;
    }

    @Override
    public void enable(Feature... features) {
        for (var feature : features) {
            if (this.features.add(feature)) {
                feature.configureWriterPipeline(this.pipeline);
            }
        }
    }

    @Override
    public void disable(Feature... features) {
        List.of(features).forEach(this.features::remove);

        this.initializePipeline();

        this.features.forEach(feature -> feature.configureWriterPipeline(this.pipeline));
    }

    @Override
    public Set<Feature> getEnabledFeatures() {
        return Collections.unmodifiableSet(this.features);
    }

    @Override
    public boolean isOptionalFeature(Feature... features) {
        for (var feature : features) {
            if (this.implicitFeatures.contains(feature)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public ByteBuf discard(long n) {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_DISCARD))
                .writeMapHeader(1)
                .writeString("n")
                .writeInt(n)
                .getTarget();
    }

    @Override
    public ByteBuf pull(long n) {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_PULL))
                .writeMapHeader(1)
                .writeString("n")
                .writeInt(n)
                .getTarget();
    }

    @Override
    public ByteBuf pull(long n, long qid) {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_PULL))
                .writeMapHeader(2)
                .writeString("n")
                .writeInt(n)
                .writeString("qid")
                .writeInt(qid)
                .getTarget();
    }

    @Override
    public ByteBuf rollback() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(0, MESSAGE_TAG_ROLLBACK))
                .getTarget();
    }

    @Override
    public ByteBuf commit() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(0, MESSAGE_TAG_COMMIT))
                .getTarget();
    }

    @Override
    public ByteBuf reset() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(0, MESSAGE_TAG_RESET))
                .getTarget();
    }

    @Override
    public ByteBuf goodbye() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(0, MESSAGE_TAG_GOODBYE))
                .getTarget();
    }

    @Override
    public ByteBuf route(RoutingContext context, Collection<String> bookmarks, String db, String impersonatedUser) {
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

        var meta = new HashMap<String, Object>();
        if (db != null) {
            meta.put("db", db);
        }
        if (impersonatedUser != null) {
            meta.put("imp_user", impersonatedUser);
        }

        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, MESSAGE_TAG_ROUTE))
                .writeMap(routingParams, PackstreamBuf::writeString)
                .writeList(bookmarkStrings, PackstreamBuf::writeString)
                .writeMap(meta)
                .getTarget();
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
                .writeString("break_decoding")
                .writeString(elementId);
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
                .writeString("break_decoding")
                .writeString(elementId)
                .writeString(startElementId)
                .writeString(endElementId);
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
                .writeString("break_decoding")
                .writeString(elementId);
    }

    @Override
    public ByteBuf logoff() {
        return PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(0, MESSAGE_TAG_LOGOFF))
                .getTarget();
    }
}
