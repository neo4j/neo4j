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

package org.neo4j.bolt.protocol.v51.decoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.v51.message.decoder.LogoffMessageDecoder;
import org.neo4j.bolt.protocol.v51.message.request.LogoffMessage;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public class LogoffMessageDecoderTest {
    @Test
    void shouldReadMessage() throws PackstreamReaderException {
        var connection = ConnectionMockFactory.newInstance();
        var decoder = LogoffMessageDecoder.getInstance();

        var msg1 =
                decoder.read(connection, PackstreamBuf.allocUnpooled(), new StructHeader(0, LogoffMessage.SIGNATURE));
        var msg2 =
                decoder.read(connection, PackstreamBuf.allocUnpooled(), new StructHeader(0, LogoffMessage.SIGNATURE));

        assertThat(msg1).isNotNull().isSameAs(LogoffMessage.INSTANCE).isSameAs(msg2);

        verifyNoMoreInteractions(connection);
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenNonEmptyStructIsGiven() {
        var connection = ConnectionMockFactory.newInstance();
        var decoder = LogoffMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(
                        connection, PackstreamBuf.allocUnpooled(), new StructHeader(1, LogoffMessage.SIGNATURE)))
                .withMessage("Illegal struct size: Expected struct to be 0 fields but got 1");
    }
}
