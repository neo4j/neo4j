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
package org.neo4j.io.marshal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.ByteBufferReadableChannel;
import org.neo4j.io.fs.OutputStreamWritableChannel;

class LimitedStringChannelMarshalTest {

    @Test
    void shouldNotTruncateInputStringIfItFitsInMaxSize() throws IOException {
        var stringChannelMarshal = new LimitedStringChannelMarshal(10);
        var outputStream = new ByteArrayOutputStream();
        var writableChannel = new OutputStreamWritableChannel(outputStream);
        var inputString = "ab";

        // when
        stringChannelMarshal.marshal(inputString, writableChannel);
        var readableChannel = new ByteBufferReadableChannel(ByteBuffer.wrap(outputStream.toByteArray()));
        var unmarshalledString = stringChannelMarshal.unmarshal(readableChannel);

        // then
        assertThat(inputString).isEqualTo(unmarshalledString);
    }

    @Test
    void shouldTruncateInputStringIfItNotFitsInMaxSize() throws IOException {
        var stringChannelMarshal = new LimitedStringChannelMarshal(2);
        var outputStream = new ByteArrayOutputStream();
        var writableChannel = new OutputStreamWritableChannel(outputStream);
        var inputString = "abc";

        // when
        stringChannelMarshal.marshal(inputString, writableChannel);
        var readableChannel = new ByteBufferReadableChannel(ByteBuffer.wrap(outputStream.toByteArray()));
        var unmarshalledString = stringChannelMarshal.unmarshal(readableChannel);

        // then
        assertThat(unmarshalledString).isEqualTo("ab");
    }

    @Test
    void ifWritableChanelExceedMaxSizeThenItShouldBeTruncated() throws IOException {
        // given
        var outputStream = new ByteArrayOutputStream();
        var writableChannel = new OutputStreamWritableChannel(outputStream);

        // serialize input string
        writableChannel.putInt(3);
        writableChannel.put("abc".getBytes(StandardCharsets.UTF_8), 3);

        // when
        var readableChannel = new ByteBufferReadableChannel(ByteBuffer.wrap(outputStream.toByteArray()));
        var unmarshalledString = new LimitedStringChannelMarshal(2).unmarshal(readableChannel);

        // then
        assertThat(unmarshalledString).isEqualTo("ab");
    }
}
