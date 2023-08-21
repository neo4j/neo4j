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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.codec.ShortStringCodec.DATE;
import static org.neo4j.internal.codec.ShortStringCodec.NUMERICAL;
import static org.neo4j.internal.codec.ShortStringCodec.bitMask;

import org.junit.jupiter.api.Test;

class ShortStringCodecTest {
    @Test
    void testMasks() {
        assertEquals(0, 1 & ~bitMask(NUMERICAL));
        assertEquals(0, 2 & ~bitMask(DATE));
        assertEquals(NUMERICAL.bitMask(), 3 & ~bitMask(DATE));
        assertEquals(0, NUMERICAL.bitMask() & ~bitMask(NUMERICAL, DATE));
    }
}
