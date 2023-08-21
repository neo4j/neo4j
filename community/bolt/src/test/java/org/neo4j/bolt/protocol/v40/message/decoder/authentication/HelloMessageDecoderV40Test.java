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
package org.neo4j.bolt.protocol.v40.message.decoder.authentication;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.AbstractHelloMessageDecoderTest;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValueBuilder;

class HelloMessageDecoderV40Test extends AbstractHelloMessageDecoderTest<HelloMessageDecoderV40> {

    @Override
    public HelloMessageDecoderV40 getDecoder() {
        return HelloMessageDecoderV40.getInstance();
    }

    @Test
    protected void shouldReadMessage() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("scheme", Values.stringValue("none"));
        var list = ListValueBuilder.newListBuilder(1);
        list.add(Values.stringValue("HINT"));
        meta.add("notifications_minimum_severity", Values.stringValue("WARNING"));
        meta.add("notifications_disabled_categories", list.build());

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42));

        Assertions.assertThat(msg).isNotNull();
        Assertions.assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        Assertions.assertThat(msg.authToken()).hasSize(1).containsEntry("scheme", "none");
        Assertions.assertThat(msg.notificationsConfig()).isNull();

        // ensure that readPrimitiveMap is the only interaction point on PackstreamValueReader as HELLO explicitly
        // forbids the use of complex structures (such as dates, points, etc) to reduce potential attack vectors that
        // could lead to denial of service attacks
        Mockito.verify(reader).readPrimitiveMap(Mockito.anyLong());
        Mockito.verifyNoMoreInteractions(reader);
    }
}
