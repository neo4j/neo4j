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

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.PageCursorUtil.get3BInt;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.getUnsignedShort;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.put3BInt;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.putUnsignedShort;

/**
 * Depending on page size {@link TreeNodeDynamicSize} need a various number of bytes to encode the offsets
 * in offset array and for the various meta-data in the header.
 * This class describe the different formats.
 */
enum DynamicSizeOffsetFormat
{
    OFFSET_2B( 2 )
            {
                @Override
                void putOffset( PageCursor cursor, int offsetValue )
                {
                    putUnsignedShort( cursor, offsetValue );
                }

                @Override
                void putOffset( PageCursor cursor, int offset, int offsetValue )
                {
                    putUnsignedShort( cursor, offset, offsetValue );
                }

                @Override
                int getOffset( PageCursor cursor )
                {
                    return getUnsignedShort( cursor );
                }

                @Override
                int getOffset( PageCursor cursor, int atOffset )
                {
                    return getUnsignedShort( cursor, atOffset );
                }
            },
    OFFSET_3B( 3 )
            {
                @Override
                void putOffset( PageCursor cursor, int offsetValue )
                {
                    put3BInt( cursor, offsetValue );
                }

                @Override
                void putOffset( PageCursor cursor, int offset, int offsetValue )
                {
                    put3BInt( cursor, offset, offsetValue );
                }

                @Override
                int getOffset( PageCursor cursor )
                {
                    return get3BInt( cursor );
                }

                @Override
                int getOffset( PageCursor cursor, int atOffset )
                {
                    return PageCursorUtil.get3BInt( cursor, atOffset );
                }
            };

    private final int offsetSize;
    private final int bytePosAllocOffset;
    private final int bytePosDeadSpace;
    private final int headerLengthDynamic;

    DynamicSizeOffsetFormat( int offsetSize )
    {
        this.offsetSize = offsetSize;
        this.bytePosAllocOffset = TreeNode.BASE_HEADER_LENGTH;
        this.bytePosDeadSpace = bytePosAllocOffset + offsetSize;
        this.headerLengthDynamic = bytePosDeadSpace + offsetSize;
    }

    /**
     * Put offset value at current position.
     */
    abstract void putOffset( PageCursor cursor, int offsetValue );

    /**
     * Put offset value at given offset.
     */
    abstract void putOffset( PageCursor cursor, int offset, int offsetValue );

    /**
     * Read encoded offset at current position.
     */
    abstract int getOffset( PageCursor cursor );

    /**
     * Read encoded offset at given offset.
     */
    abstract int getOffset( PageCursor cursor, int atOffset );

    /**
     * Number of bytes used to encode an offset.
     */
    int offsetSize()
    {
        return offsetSize;
    }

    /**
     * Byte position for the allocOffset header field.
     */
    int getBytePosAllocOffset()
    {
        return bytePosAllocOffset;
    }

    /**
     * Byte position for the deadSpace header field.
     */
    int getBytePosDeadSpace()
    {
        return bytePosDeadSpace;
    }

    /**
     * Total length of header and also starting offset for data-section.
     */
    int getHeaderLength()
    {
        return headerLengthDynamic;
    }
}
