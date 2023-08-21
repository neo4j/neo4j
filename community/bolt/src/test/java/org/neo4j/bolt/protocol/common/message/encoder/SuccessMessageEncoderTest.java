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
package org.neo4j.bolt.protocol.common.message.encoder;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.message.response.SuccessMessage;
import org.neo4j.bolt.protocol.io.pipeline.PipelineContext;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;

class SuccessMessageEncoderTest {

    @Test
    void shouldWriteSimpleMessage() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var encoder = SuccessMessageEncoder.getInstance();
        var ctx = Mockito.mock(PipelineContext.class);
        var connection =
                ConnectionMockFactory.newFactory().withWriterContext(ctx).build();

        var metaBuilder = new MapValueBuilder();
        metaBuilder.add("the_answer", Values.longValue(42));
        metaBuilder.add("foo", Values.stringValue("bar"));
        var expectedMeta = metaBuilder.build();

        encoder.write(connection, buf, new SuccessMessage(expectedMeta));

        Mockito.verify(ctx).writeValue(expectedMeta);
    }
}
