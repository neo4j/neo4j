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
package org.neo4j.bolt.protocol.common.message.decoder.connection;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.message.decoder.MultiParameterMessageDecoderTest;
import org.neo4j.bolt.protocol.common.message.request.connection.RouteMessage;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

public class LegacyRouteMessageDecoderTest implements MultiParameterMessageDecoderTest<LegacyRouteMessageDecoder> {

    @Override
    public LegacyRouteMessageDecoder getDecoder() {
        return LegacyRouteMessageDecoder.getInstance();
    }

    @Override
    public int minimumNumberOfFields() {
        return 3;
    }

    @Test
    void shouldReadMessage() throws PackstreamReaderException {
        var reader = Mockito.mock(PackstreamValueReader.class);

        var builder = new MapValueBuilder();
        builder.add("address", Values.stringValue("potato.example.org"));
        var routingContext = builder.build();

        var buf = Mockito.spy(PackstreamBuf.allocUnpooled());
        Mockito.doReturn(Type.LIST).when(buf).peekType();

        Mockito.doReturn(routingContext).when(reader).readMap();
        Mockito.doReturn(VirtualValues.EMPTY_LIST).when(reader).readList();
        Mockito.doReturn(Values.stringValue("neo5j")).when(reader).readValue();

        var connection = ConnectionMockFactory.newFactory()
                .withConnector(connector -> {})
                .withValueReader(reader)
                .build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(3, RouteMessage.SIGNATURE));

        Assertions.assertThat(msg).isNotNull();

        Assertions.assertThat(msg.getBookmarks()).isEmpty();
        Assertions.assertThat(msg.getDatabaseName()).isEqualTo("neo5j");
        Assertions.assertThat(msg.getRequestContext()).isSameAs(routingContext);
    }
}
