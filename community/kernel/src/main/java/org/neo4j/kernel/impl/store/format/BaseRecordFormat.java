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
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.id.validation.IdValidator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;

/**
 * Basic abstract implementation of a {@link RecordFormat} implementing most functionality except
 * {@link RecordFormat#read(AbstractBaseRecord, PageCursor, org.neo4j.kernel.impl.store.record.RecordLoad, int)} and
 * {@link RecordFormat#write(AbstractBaseRecord, PageCursor, int)}.
 *
 * @param <RECORD> type of record.
 */
public abstract class BaseRecordFormat<RECORD extends AbstractBaseRecord> implements RecordFormat<RECORD>
{
    public static final int IN_USE_BIT = 0b0000_0001;
    public static final Function<StoreHeader,Integer> INT_STORE_HEADER_READER =
            header -> ((IntStoreHeader) header).value();

    public static Function<StoreHeader,Integer> fixedRecordSize( int recordSize )
    {
        return header -> recordSize;
    }

    private final Function<StoreHeader,Integer> recordSize;
    private final int recordHeaderSize;
    private final long maxId;

    protected BaseRecordFormat( Function<StoreHeader,Integer> recordSize, int recordHeaderSize, int idBits )
    {
        this.recordSize = recordSize;
        this.recordHeaderSize = recordHeaderSize;
        this.maxId = (1L << idBits) - 1;
    }

    @Override
    public int getRecordSize( StoreHeader header )
    {
        return recordSize.apply( header );
    }

    @Override
    public int getRecordHeaderSize()
    {
        return recordHeaderSize;
    }

    @Override
    public long getNextRecordReference( RECORD record )
    {
        return Record.NULL_REFERENCE.intValue();
    }

    public static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && IdValidator.isReservedId( base ) ? -1 : base | modifier;
    }

    @Override
    public void prepare( RECORD record, int recordSize, IdSequence idSequence )
    {   // Do nothing by default
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && getClass().equals( obj.getClass() );
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();

    }

    @Override
    public final long getMaxId()
    {
        return maxId;
    }
}
