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
package org.neo4j.bolt.protocol.common.message.decoder;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.authentication.LogoffMessage;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public interface SingletonMessageDecoderTest<D extends MessageDecoder<M>, M extends RequestMessage>
        extends MessageDecoderTest<D> {

    @Override
    default int maximumNumberOfFields() {
        return 0;
    }

    M getMessageSingleton();

    @Test
    default void shouldReadMessage() throws PackstreamReaderException {
        var connection1 = ConnectionMockFactory.newInstance();
        var connection2 = ConnectionMockFactory.newInstance();
        var decoder = this.getDecoder();

        var msg1 =
                decoder.read(connection1, PackstreamBuf.allocUnpooled(), new StructHeader(0, LogoffMessage.SIGNATURE));
        var msg2 =
                decoder.read(connection1, PackstreamBuf.allocUnpooled(), new StructHeader(0, LogoffMessage.SIGNATURE));
        var msg3 =
                decoder.read(connection2, PackstreamBuf.allocUnpooled(), new StructHeader(0, LogoffMessage.SIGNATURE));

        Assertions.assertThat(msg1)
                .isSameAs(this.getMessageSingleton())
                .isSameAs(msg2)
                .isSameAs(msg3);

        Mockito.verifyNoInteractions(connection1);
        Mockito.verifyNoInteractions(connection2);
    }
}
