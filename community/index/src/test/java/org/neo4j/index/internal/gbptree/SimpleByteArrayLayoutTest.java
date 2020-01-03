/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.index.internal.gbptree;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class SimpleByteArrayLayoutTest
{
    private final SimpleByteArrayLayout layout = new SimpleByteArrayLayout( false );
    private final RawBytes left = layout.newKey();
    private final RawBytes right = layout.newKey();
    private final RawBytes minimalSplitter = layout.newKey();

    @Test
    void minimalSplitterLengthZero()
    {
        left.bytes = new byte[0];
        right.bytes = new byte[1];

        layout.minimalSplitter( left, right, minimalSplitter );

        assertArrayEquals( right.bytes, minimalSplitter.bytes );
    }

    @Test
    void minimalSplitterLengthZeroWithLongRight()
    {

        left.bytes = new byte[0];
        right.bytes = new byte[]{1, 1};
        layout.minimalSplitter( left, right, minimalSplitter );

        assertArrayEquals( new byte[]{1}, minimalSplitter.bytes );
    }

    @Test
    void minimalSplitterLengthOne()
    {
        left.bytes = new byte[]{0};
        right.bytes = new byte[]{1};

        layout.minimalSplitter( left, right, minimalSplitter );

        assertArrayEquals( right.bytes, minimalSplitter.bytes );
    }

    @Test
    void minimalSplitterDifferOnLength()
    {
        left.bytes = new byte[1];
        right.bytes = new byte[2];

        layout.minimalSplitter( left, right, minimalSplitter );

        assertArrayEquals( right.bytes, minimalSplitter.bytes );
    }

    @Test
    void minimalSplitterDifferMoreOnLength()
    {
        left.bytes = new byte[1];
        right.bytes = new byte[3];

        layout.minimalSplitter( left, right, minimalSplitter );

        assertArrayEquals( new byte[2], minimalSplitter.bytes );
    }

    @Test
    void minimalSplitterDifferOnLast()
    {
        left.bytes = new byte[]{0,0};
        right.bytes = new byte[]{0,1};

        layout.minimalSplitter( left, right, minimalSplitter );

        assertArrayEquals( right.bytes, minimalSplitter.bytes );
    }

    @Test
    void minimalSplitterDifferOnFirst()
    {
        left.bytes = new byte[]{0,0};
        right.bytes = new byte[]{1,0};

        layout.minimalSplitter( left, right, minimalSplitter );

        assertArrayEquals( new byte[]{1}, minimalSplitter.bytes );
    }

    @Test
    void minimalSplitterDifferOnFirstLeftShorter()
    {
        left.bytes = new byte[]{0};
        right.bytes = new byte[]{1,0};

        layout.minimalSplitter( left, right, minimalSplitter );

        assertArrayEquals( new byte[]{1}, minimalSplitter.bytes );
    }

    @Test
    void minimalSplitterDifferOnFirstRightShorter()
    {
        left.bytes = new byte[]{0,0};
        right.bytes = new byte[]{1};

        layout.minimalSplitter( left, right, minimalSplitter );

        assertArrayEquals( right.bytes, minimalSplitter.bytes );
    }
}
