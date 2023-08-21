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
package org.neo4j.bolt.protocol.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.io.pipeline.WriterContext;
import org.neo4j.bolt.protocol.io.writer.LegacyStructWriter;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;

// TODO: Remove along with merge of DefaultBoltValueWriter and LegacyBoltValueWriter - Coverage in remaining tests
@Deprecated(since = "5.0", forRemoval = true)
class LegacyStructWriterTest {

    @Test
    void shouldWriteNode() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var ctx = Mockito.mock(WriterContext.class);

        Mockito.doReturn(buf).when(ctx).buffer();

        var propertyBuilder = new MapValueBuilder();
        propertyBuilder.add("the_answer", Values.intValue(42));
        propertyBuilder.add(
                "some_unrelated_string", Values.stringValue("What do you get when you multiply six by nine"));
        var properties = propertyBuilder.build();

        LegacyStructWriter.getInstance().writeNode(ctx, "42", 42, stringArray("foo", "bar", "baz"), properties, false);

        var header = buf.readStructHeader();
        var nodeId = buf.readInt();
        var labels = buf.readList(PackstreamBuf::readString);
        Mockito.verify(ctx).writeValue(properties);

        assertThat(header.tag()).isEqualTo((short) 'N');
        assertThat(header.length()).isEqualTo(3);

        assertThat(nodeId).isEqualTo(42L);

        assertThat(labels).hasSize(3).containsExactly("foo", "bar", "baz");
    }

    @Test
    void shouldWriteRelationship() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var ctx = Mockito.mock(WriterContext.class);

        Mockito.doReturn(buf).when(ctx).buffer();

        var propertyBuilder = new MapValueBuilder();
        propertyBuilder.add("the_answer", Values.intValue(42));
        propertyBuilder.add(
                "some_unrelated_string", Values.stringValue("What do you get when you multiply six by nine"));
        var properties = propertyBuilder.build();

        LegacyStructWriter.getInstance()
                .writeRelationship(
                        ctx, "42", 42, "21", 21, "84", 84, stringValue("LIKES_WORKING_WITH"), properties, false);

        var header = buf.readStructHeader();
        var relationshipId = buf.readInt();
        var startNodeId = buf.readInt();
        var endNodeId = buf.readInt();
        var type = buf.readString();
        Mockito.verify(ctx).writeValue(properties);

        assertThat(header.tag()).isEqualTo((short) 'R');
        assertThat(header.length()).isEqualTo(5);

        assertThat(relationshipId).isEqualTo(42L);
        assertThat(startNodeId).isEqualTo(21);
        assertThat(endNodeId).isEqualTo(84);
        assertThat(type).isEqualTo("LIKES_WORKING_WITH");
    }

    @Test
    void shouldWriteUnboundRelationship() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var ctx = Mockito.mock(WriterContext.class);

        Mockito.doReturn(buf).when(ctx).buffer();

        var propertyBuilder = new MapValueBuilder();
        propertyBuilder.add("the_answer", Values.intValue(42));
        propertyBuilder.add(
                "some_unrelated_string", Values.stringValue("What do you get when you multiply six by nine"));
        var properties = propertyBuilder.build();

        LegacyStructWriter.getInstance().writeUnboundRelationship(ctx, "42", 42, "LIKES_WORKING_WITH", properties);

        var header = buf.readStructHeader();
        var relationshipId = buf.readInt();
        var type = buf.readString();
        Mockito.verify(ctx).writeValue(properties);

        assertThat(header.tag()).isEqualTo((short) 'r');
        assertThat(header.length()).isEqualTo(3);

        assertThat(relationshipId).isEqualTo(42L);
        assertThat(type).isEqualTo("LIKES_WORKING_WITH");
    }
}
