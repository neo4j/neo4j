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
package org.neo4j.bolt.protocol.v40.messaging.decoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.connection.BoltConnection;
import org.neo4j.bolt.protocol.v40.messaging.request.GoodbyeMessage;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

class GoodbyeMessageDecoderTest {

    @Test
    void shouldReadMessage() throws PackstreamReaderException {
        var connection = mock(BoltConnection.class);
        var decoder = new GoodbyeMessageDecoder(connection);

        var msg1 = decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42));
        var msg2 = decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42));

        assertThat(msg1).isNotNull().isSameAs(GoodbyeMessage.INSTANCE).isSameAs(msg2);

        verify(connection, times(2)).stop();
        verifyNoMoreInteractions(connection);
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenNonEmptyStructIsGiven() {
        var decoder = new GoodbyeMessageDecoder(mock(BoltConnection.class));

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 0 fields but got 1");
    }
}
