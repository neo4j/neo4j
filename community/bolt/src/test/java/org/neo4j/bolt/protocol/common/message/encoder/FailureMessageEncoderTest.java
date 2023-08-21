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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.message.response.FailureMessage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.testing.PackstreamBufAssertions;

class FailureMessageEncoderTest {

    @Test
    void shouldWriteSimpleMessage() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var encoder = FailureMessageEncoder.getInstance();

        encoder.write(null, buf, new FailureMessage(Status.Request.InvalidFormat, "Something went wrong! :(", false));

        PackstreamBufAssertions.assertThat(buf).containsMap(meta -> assertThat(meta)
                .isNotNull()
                .hasSize(2)
                .containsEntry("code", Status.Request.InvalidFormat.code().serialize())
                .containsEntry("message", "Something went wrong! :("));

        assertThat(buf.getTarget().isReadable()).isFalse();
    }
}
