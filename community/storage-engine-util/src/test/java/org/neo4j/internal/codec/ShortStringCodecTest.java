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
package org.neo4j.internal.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.codec.ShortStringCodec.CODEC_COUNT;
import static org.neo4j.internal.codec.ShortStringCodec.DATE;
import static org.neo4j.internal.codec.ShortStringCodec.NUMERICAL;
import static org.neo4j.internal.codec.ShortStringCodec.bitMask;

import org.junit.jupiter.api.Test;

class ShortStringCodecTest {
    @Test
    void shouldComputeNonOverlappingBitMasksForCodecs() {
        assertThat(1 & ~bitMask(NUMERICAL)).isZero();
        assertThat(2 & ~bitMask(DATE)).isZero();
        assertThat(3 & ~bitMask(DATE)).isEqualTo(NUMERICAL.bitMask());
        assertThat(NUMERICAL.bitMask() & ~bitMask(NUMERICAL, DATE)).isZero();
    }

    @Test
    void shouldOfferOnlyCodecsThatCanEncodeTheCharacter() {
        for (int c = 0; c < 256; c++) {
            int packed = ShortStringCodec.packedFor(c);
            byte translated = (byte) packed;
            int codecs = packed >>> Byte.SIZE;

            for (ShortStringCodec codec : ShortStringCodec.CODECS) {
                if ((codecs & codec.bitMask()) != 0) {
                    assertThat(codec.encTable()[translated & 0xFF])
                            .as(
                                    "character %d translates to 0x%02X, which %s is offered and must encode",
                                    c, translated & 0xFF, codec)
                            .isNotEqualTo(ShortStringCodec.CANNOT_ENCODE);
                }
            }
        }
    }

    @Test
    void shouldPackTheCodecMaskClearOfTheTranslationByte() {
        assertThat(Integer.numberOfLeadingZeros(ShortStringCodec.ALL_BIT_MASK))
                .as("room above the translation byte for %d codec bits", CODEC_COUNT)
                .isGreaterThanOrEqualTo(Byte.SIZE);

        for (int c = 0; c < 256; c++) {
            int packed = ShortStringCodec.packedFor(c);
            assertThat(packed >>> Byte.SIZE)
                    .as("codec mask for character %d stays within ALL_BIT_MASK", c)
                    .isEqualTo((packed >>> Byte.SIZE) & ShortStringCodec.ALL_BIT_MASK);
        }
    }

    @Test
    void shouldAgreeWithEncTranslateForEveryCodecAndEveryInput() {
        for (ShortStringCodec codec : ShortStringCodec.CODECS) {
            int[] table = codec.encTable();
            assertThat(table).as("%s has a table", codec).hasSize(256);

            for (int value = 0; value < 256; value++) {
                assertThat(table[value]).as("%s encodes input %d", codec, value).isEqualTo(codec.encTranslate((byte)
                        value));
            }
        }
    }
}
