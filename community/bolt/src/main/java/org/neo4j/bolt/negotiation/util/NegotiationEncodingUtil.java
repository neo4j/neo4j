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
package org.neo4j.bolt.negotiation.util;

import io.netty.buffer.ByteBuf;

public final class NegotiationEncodingUtil {
    private NegotiationEncodingUtil() {}

    public static void writeBitMask(ByteBuf buf, BitMask mask) {
        var totalBits = mask.length();
        var encodedLength = totalBits / 7 + (totalBits % 7 == 0 ? 0 : 1);

        for (var i = 0; i < encodedLength; i++) {
            var b = mask.readN(Math.min(7, mask.readable()));
            if (i + 1 < encodedLength) {
                b ^= 0x80;
            }

            buf.writeByte(b);
        }
    }

    public static boolean isBitMaskReadable(ByteBuf buf, int limit) {
        // explicit replacement of markReaderIndex() and resetReaderIndex so that this method does not
        // introduce any side effects
        var readerIndex = buf.readerIndex();

        try {
            byte recv;
            for (var i = 0; i < limit; i++) {
                if (!buf.isReadable()) {
                    return false;
                }

                recv = buf.readByte();

                if ((recv & 0x80) == 0x00) {
                    return true;
                }
            }
        } finally {
            buf.readerIndex(readerIndex);
        }

        return false;
    }

    public static BitMask readBitMask(ByteBuf buf) {
        var recv = buf.alloc().buffer();
        try {
            byte i;
            do {
                i = buf.readByte();
                recv.writeByte(i);
            } while ((i & 0x80) != 0x00);

            var mask = new BitMask(buf.alloc(), recv.readableBytes() * 7);

            do {
                var b = recv.readByte();
                mask.writeN(b, 7);
            } while (recv.isReadable());
            return mask;
        } finally {
            recv.release();
        }
    }

    public static void writeVarInt(ByteBuf buf, int value) {
        do {
            var b = value & 0x7F;
            value >>>= 7;

            if (value != 0) {
                b ^= 0x80;
            }

            buf.writeByte(b);
        } while (value != 0);
    }
}
