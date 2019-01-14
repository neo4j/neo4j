/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.store.format;

import java.util.function.Function;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * Implementation of a very common type of format where the first byte, at least one bit in it,
 * say whether or not the record is in use. That can be used to let sub classes have simpler
 * read/write implementations. The rest of the 7 bits in that header byte are free to use by subclasses.
 *
 * @param <RECORD> type of record.
 */
public abstract class BaseOneByteHeaderRecordFormat<RECORD extends AbstractBaseRecord> extends BaseRecordFormat<RECORD>
{
    private final int inUseBitMaskForFirstByte;

    protected BaseOneByteHeaderRecordFormat( Function<StoreHeader,Integer> recordSize, int recordHeaderSize,
            int inUseBitMaskForFirstByte, int idBits )
    {
        super( recordSize, recordHeaderSize, idBits );
        this.inUseBitMaskForFirstByte = inUseBitMaskForFirstByte;
    }

    protected void markAsUnused( PageCursor cursor )
    {
        byte inUseByte = cursor.getByte( cursor.getOffset() );
        inUseByte &= ~inUseBitMaskForFirstByte;
        cursor.putByte( inUseByte );
    }

    @Override
    public boolean isInUse( PageCursor cursor )
    {
        return isInUse( cursor.getByte( cursor.getOffset() ) );
    }

    /**
     * Given a record with a header byte this method checks the specific bit which this record format was
     * configured to interpret as inUse.
     *
     * @param headerByte header byte of a record (the first byte) which contains the inUse bit we're interested in.
     * @return whether or not this header byte has the specific bit saying that it's in use.
     */
    protected boolean isInUse( byte headerByte )
    {
        return has( headerByte, inUseBitMaskForFirstByte );
    }

    /**
     * Checks whether or not a specific bit in a byte is set.
     *
     * @param headerByte the header byte to check, here represented as a {@code long} for convenience
     * due to many callers keeping this header as long as to remove common problems of forgetting to
     * cast to long before shifting.
     * @param bitMask mask for the bit to check, such as 0x1, 0x2 and 0x4.
     * @return whether or not that bit is set.
     */
    protected static boolean has( long headerByte, int bitMask )
    {
        return (headerByte & bitMask) != 0;
    }

    /**
     * Sets or clears bits specified by the {@code bitMask} in the header byte.
     *
     * @param headerByte byte to set bits in.
     * @param bitMask mask specifying which bits to change.
     * @param value {@code true} means setting the bits specified by the bit mask, {@code false} means clearing.
     * @return the {@code headerByte} with the changes incorporated.
     */
    protected static byte set( byte headerByte, int bitMask, boolean value )
    {
        return (byte) (value ? headerByte | bitMask : headerByte);
    }
}
