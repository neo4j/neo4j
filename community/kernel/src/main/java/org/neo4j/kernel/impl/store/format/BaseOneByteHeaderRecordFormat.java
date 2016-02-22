/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
            int inUseBitMaskForFirstByte )
    {
        super( recordSize, recordHeaderSize );
        this.inUseBitMaskForFirstByte = inUseBitMaskForFirstByte;
    }

    protected void markFirstByteAsUnused( PageCursor cursor )
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

    protected boolean isInUse( byte firstByte )
    {
        return (firstByte & inUseBitMaskForFirstByte) != 0;
    }

    protected static boolean has( long headerByte, int bitMask )
    {
        return (headerByte & bitMask) != 0;
    }

    protected static byte set( byte header, int bitMask, boolean value )
    {
        return (byte) (value ? header | bitMask : header);
    }
}
