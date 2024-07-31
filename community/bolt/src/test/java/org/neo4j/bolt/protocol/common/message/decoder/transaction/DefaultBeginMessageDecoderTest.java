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
package org.neo4j.bolt.protocol.common.message.decoder.transaction;

import java.time.Duration;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.notifications.SelectiveNotificationsConfig;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

public class DefaultBeginMessageDecoderTest extends AbstractBeginMessageDecoderTest<DefaultBeginMessageDecoder> {

    @Override
    public DefaultBeginMessageDecoder getDecoder() {
        return DefaultBeginMessageDecoder.getInstance();
    }

    @Override
    public int maximumNumberOfFields() {
        return 1;
    }

    @Test
    void shouldReadMessage() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var txMetadata = new MapValueBuilder();
        txMetadata.add("foo", Values.stringValue("bar"));
        txMetadata.add("baz", Values.longValue(4));

        var meta = new MapValueBuilder();
        meta.add(
                "bookmarks",
                VirtualValues.list(
                        Values.stringValue("neo4j:mock:bookmark1"), Values.stringValue("neo4j:mock:bookmark2")));
        meta.add("tx_timeout", Values.longValue(42));
        meta.add("mode", Values.stringValue("w"));
        meta.add("tx_metadata", txMetadata.build());
        meta.add("db", Values.stringValue("neo4j"));
        meta.add("imp_user", Values.stringValue("bob"));
        meta.add("tx_type", Values.stringValue("IMPLICIT"));
        var list = ListValueBuilder.newListBuilder(1);
        list.add(Values.stringValue("HINT"));
        meta.add("notifications_minimum_severity", Values.stringValue("WARNING"));
        meta.add("notifications_disabled_classifications", list.build());

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42));

        Assertions.assertThat(msg).isNotNull();
        Assertions.assertThat(msg.bookmarks())
                .containsExactlyInAnyOrder("neo4j:mock:bookmark1", "neo4j:mock:bookmark2");
        Assertions.assertThat(msg.transactionTimeout()).isEqualByComparingTo(Duration.ofMillis(42));
        Assertions.assertThat(msg.getAccessMode()).isEqualTo(AccessMode.WRITE);
        Assertions.assertThat(msg.transactionMetadata())
                .containsEntry("foo", "bar")
                .containsEntry("baz", 4L);
        Assertions.assertThat(msg.databaseName()).isEqualTo("neo4j");
        Assertions.assertThat(msg.impersonatedUser()).isEqualTo("bob");
        Assertions.assertThat(msg.type()).isEqualTo(TransactionType.IMPLICIT);
        Assertions.assertThat(msg.notificationsConfig())
                .isEqualTo(new SelectiveNotificationsConfig("WARNING", List.of("HINT")));
    }

    @Test
    void shouldFallbackToExplicitTypeWhenPropertyIsOmitted() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        Mockito.doReturn(MapValue.EMPTY).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42));

        Assertions.assertThat(msg).isNotNull().extracting(BeginMessage::type).isEqualTo(TransactionType.EXPLICIT);
    }
}
