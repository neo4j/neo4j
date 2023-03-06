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
package org.neo4j.bolt.protocol.common.message.decoder.authentication;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;

public class DefaultHelloMessageDecoderTest extends AbstractHelloMessageDecoderTest<DefaultHelloMessageDecoder> {

    @Override
    public DefaultHelloMessageDecoder getDecoder() {
        return DefaultHelloMessageDecoder.getInstance();
    }

    protected void appendRequiredFields(MapValueBuilder meta) {
        // HELLO no longer includes authentication information
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
    }

    @Test
    void shouldReadMessage() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var builder = new MapValueBuilder();
        builder.add("address", Values.stringValue("localhost"));
        var routing = builder.build();

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("routing", routing);

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42));

        Assertions.assertThat(msg).isNotNull();
        // This assumes that it will be a Normal String and not a neo StringValue.
        Assertions.assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");

        Assertions.assertThat(msg.routingContext()).isNotNull().satisfies(ctx -> {
            Assertions.assertThat(ctx.isServerRoutingEnabled()).isTrue();

            Assertions.assertThat(ctx.getParameters()).hasSize(1).containsEntry("address", "localhost");
        });

        // ensure that readPrimitiveMap is the only interaction point on PackstreamValueReader as HELLO explicitly
        // forbids the use of complex structures (such as dates, points, etc) to reduce potential attack vectors that
        // could lead to denial of service attacks
        Mockito.verify(reader).readPrimitiveMap(Mockito.anyLong());
        Mockito.verifyNoMoreInteractions(reader);
    }

    @Override
    protected void shouldConvertSensitiveValues() {
        // HELLO no longer includes authentication information
    }
}
