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
import static org.mockito.ArgumentMatchers.eq;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.io.pipeline.WriterContext;
import org.neo4j.bolt.protocol.io.writer.DefaultStructWriter;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

class DefaultBoltValueWriterTest {

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

        DefaultStructWriter.getInstance()
                .writeNode(ctx, "42", 42, stringArray("foo", "bar", "baz"), propertyBuilder.build(), false);

        var header = buf.readStructHeader();
        var nodeId = buf.readInt();
        var labels = buf.readList(PackstreamBuf::readString);
        Mockito.verify(ctx).writeValue(properties);
        var elementId = buf.readString();

        assertThat(header.tag()).isEqualTo((short) 'N');
        assertThat(header.length()).isEqualTo(4);

        assertThat(nodeId).isEqualTo(42L);
        assertThat(elementId).isEqualTo("42");

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

        DefaultStructWriter.getInstance()
                .writeRelationship(
                        ctx, "42", 42, "21", 21, "84", 84, stringValue("LIKES_WORKING_WITH"), properties, false);

        var header = buf.readStructHeader();
        var relationshipId = buf.readInt();
        var startNodeId = buf.readInt();
        var endNodeId = buf.readInt();
        var type = buf.readString();
        Mockito.verify(ctx).writeValue(properties);
        var elementId = buf.readString();
        var startNodeElementId = buf.readString();
        var endNodeElementId = buf.readString();

        assertThat(header.tag()).isEqualTo((short) 'R');
        assertThat(header.length()).isEqualTo(8);

        assertThat(elementId).isEqualTo("42");
        assertThat(relationshipId).isEqualTo(42L);
        assertThat(startNodeElementId).isEqualTo("21");
        assertThat(startNodeId).isEqualTo(21);
        assertThat(endNodeElementId).isEqualTo("84");
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

        DefaultStructWriter.getInstance().writeUnboundRelationship(ctx, "42", 42, "LIKES_WORKING_WITH", properties);

        var header = buf.readStructHeader();
        var relationshipId = buf.readInt();
        var type = buf.readString();
        Mockito.verify(ctx).writeValue(properties);
        var elementId = buf.readString();

        assertThat(header.tag()).isEqualTo((short) 'r');
        assertThat(header.length()).isEqualTo(4);

        assertThat(elementId).isEqualTo("42");
        assertThat(relationshipId).isEqualTo(42L);
        assertThat(type).isEqualTo("LIKES_WORKING_WITH");
    }

    @Test
    void shouldWritePath() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var ctx = Mockito.mock(WriterContext.class);

        Mockito.doReturn(buf).when(ctx).buffer();

        var person = nodeValue(21, "21", stringArray("Person"), MapValue.EMPTY);
        var computer = nodeValue(42, "42", stringArray("Computer"), MapValue.EMPTY);
        var vendor = nodeValue(84, "84", stringArray("Vendor"), MapValue.EMPTY);

        var owns = relationshipValue(13, "13", person, computer, Values.stringValue("OWNS"), MapValue.EMPTY);
        var makes = relationshipValue(26, "26", vendor, computer, Values.stringValue("MAKES"), MapValue.EMPTY);

        var nodes = new NodeValue[] {person, computer, vendor};
        var rels = new RelationshipValue[] {owns, makes};

        DefaultStructWriter.getInstance().writePath(ctx, nodes, rels);

        var header = buf.readStructHeader();
        var nodesHeader = buf.readLengthPrefixMarker(Type.LIST);
        var relsHeader = buf.readLengthPrefixMarker(Type.LIST);

        var indices = buf.readList(PackstreamBuf::readInt);

        assertThat(header.length()).isEqualTo(3);
        assertThat(header.tag()).isEqualTo((short) 'P');

        assertThat(nodesHeader).isEqualTo(3);
        Mockito.verify(ctx).writeValue(eq(person));
        Mockito.verify(ctx).writeValue(eq(computer));
        Mockito.verify(ctx).writeValue(eq(vendor));

        assertThat(relsHeader).isEqualTo(2);
        Mockito.verify(ctx).writeUnboundRelationship("13", 13, "OWNS", MapValue.EMPTY);
        Mockito.verify(ctx).writeUnboundRelationship("26", 26, "MAKES", MapValue.EMPTY);

        assertThat(indices).hasSize(4).containsExactly(1L, 1L, -2L, 2L);
    }

    @Test
    void shouldWriteEasyPath() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var ctx = Mockito.mock(WriterContext.class);

        Mockito.doReturn(buf).when(ctx).buffer();

        {
            var person = nodeValue(21, "21", stringArray("Person"), MapValue.EMPTY);
            var computer = nodeValue(42, "42", stringArray("Computer"), MapValue.EMPTY);
            var cpu = nodeValue(84, "84", stringArray("CPU"), MapValue.EMPTY);

            var owns = relationshipValue(13, "13", person, computer, Values.stringValue("OWNS"), MapValue.EMPTY);
            var makes = relationshipValue(26, "26", computer, cpu, Values.stringValue("HAS"), MapValue.EMPTY);

            var nodes = new NodeValue[] {person, computer, cpu};
            var rels = new RelationshipValue[] {owns, makes};

            DefaultStructWriter.getInstance().writePath(ctx, nodes, rels);
        }

        var header = buf.readStructHeader();
        var nodesHeader = buf.readLengthPrefixMarker(Type.LIST);
        var relsHeader = buf.readLengthPrefixMarker(Type.LIST);
        var indices = buf.readList(PackstreamBuf::readInt);

        assertThat(header.length()).isEqualTo(3);
        assertThat(header.tag()).isEqualTo((short) 'P');

        assertThat(nodesHeader).isEqualTo(3);
        assertThat(relsHeader).isEqualTo(2);

        assertThat(indices).hasSize(4).containsExactly(1L, 1L, 2L, 2L);
    }
}
