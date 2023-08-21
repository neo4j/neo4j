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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

public class LimitedStringChannelMarshal implements ChannelMarshal<String> {
    public static final int NULL_STRING_LENGTH = -1;
    public static final LimitedStringChannelMarshal INSTANCE = new LimitedStringChannelMarshal(10);
    private final int maxStringSize;

    /**
     * @param maxStringSize - defines a maximum size of a string in bytes that will be written in the channel or read from it.
     */
    public LimitedStringChannelMarshal(int maxStringSize) {
        if (maxStringSize <= 0) {
            throw new IllegalArgumentException("maxStringSize should be more than zero");
        }
        this.maxStringSize = maxStringSize;
    }

    @Override
    public void marshal(String string, WritableChannel channel) throws IOException {
        if (string == null) {
            channel.putInt(NULL_STRING_LENGTH);
        } else {
            if (string.length() > maxStringSize) {
                string = string.substring(0, maxStringSize);
            }
            byte[] bytes = string.getBytes(UTF_8);
            channel.putInt(bytes.length);
            channel.put(bytes, bytes.length);
        }
    }

    @Override
    public String unmarshal(ReadableChannel channel) throws IOException {
        int len = channel.getInt();
        if (len == NULL_STRING_LENGTH) {
            return null;
        }
        if (len > maxStringSize) {
            len = maxStringSize;
        }

        byte[] stringBytes = new byte[len];
        channel.get(stringBytes, stringBytes.length);

        return new String(stringBytes, UTF_8);
    }
}
