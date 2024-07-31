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
package org.neo4j.bolt.protocol.common.message.decoder.authentication;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.message.notifications.SelectiveNotificationsConfig;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValueBuilder;

public class DefaultHelloMessageDecoderTest extends AbstractHelloMessageDecoderTest<DefaultHelloMessageDecoder> {

    @Override
    public DefaultHelloMessageDecoder getDecoder() {
        return DefaultHelloMessageDecoder.getInstance();
    }

    protected void appendRequiredFields(MapValueBuilder meta) {
        // HELLO no longer includes authentication information
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        var boltBuilder = new MapValueBuilder();
        boltBuilder.add("product", Values.stringValue("stub/5"));
        meta.add("bolt_agent", boltBuilder.build());
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
        var boltBuilder = new MapValueBuilder();
        boltBuilder.add("product", Values.stringValue("stub/5"));
        meta.add("bolt_agent", boltBuilder.build());
        meta.add("routing", routing);
        var list = ListValueBuilder.newListBuilder(1);
        list.add(Values.stringValue("HINT"));
        meta.add("notifications_minimum_severity", Values.stringValue("WARNING"));
        meta.add("notifications_disabled_classifications", list.build());

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42));

        Assertions.assertThat(msg).isNotNull();
        // This assumes that it will be a Normal String and not a neo StringValue.
        Assertions.assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        Assertions.assertThat(msg.boltAgent()).isEqualTo(Map.of("product", "stub/5"));

        Assertions.assertThat(msg.routingContext()).isNotNull().satisfies(ctx -> {
            Assertions.assertThat(ctx.isServerRoutingEnabled()).isTrue();

            Assertions.assertThat(ctx.getParameters()).hasSize(1).containsEntry("address", "localhost");
        });
        Assertions.assertThat(msg.notificationsConfig())
                .isEqualTo(new SelectiveNotificationsConfig("WARNING", List.of("HINT")));

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

    @Test
    protected void shouldFailWithIllegalStructArgumentWhenBoltAgentIsOmitted() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("scheme", Values.stringValue("none"));
        meta.add("user_agent", Values.stringValue("valid"));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"bolt_agent\": Must be a map with string keys and string values.");
    }

    @Test
    protected void shouldFailWithIllegalStructArgumentWhenBoltAgentIsInvalid() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("scheme", Values.stringValue("none"));
        meta.add("user_agent", Values.stringValue("valid"));
        var boltAgentBuilder = new MapValueBuilder();
        boltAgentBuilder.add("product", Values.booleanValue(true));
        meta.add("bolt_agent", boltAgentBuilder.build());

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"bolt_agent\": Must be a map with string keys and string values.");
    }

    @Test
    protected void shouldFailWithIllegalStructArgumentWhenBoltAgentMissingProductKey()
            throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("scheme", Values.stringValue("none"));
        meta.add("user_agent", Values.stringValue("valid"));
        var boltAgentBuilder = new MapValueBuilder();
        boltAgentBuilder.add("valid-key", Values.stringValue("valid types."));
        meta.add("bolt_agent", boltAgentBuilder.build());

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"bolt_agent\": Expected map to contain key: 'product'.");
    }
}
