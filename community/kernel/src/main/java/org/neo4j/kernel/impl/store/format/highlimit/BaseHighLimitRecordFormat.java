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
package org.neo4j.kernel.impl.store.format.highlimit;

import java.io.IOException;
import java.util.function.Function;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.highlimit.Reference.DataAdapter;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;

import static org.neo4j.kernel.impl.store.format.highlimit.Reference.PAGE_CURSOR_ADAPTER;

/**
 * Base class for record format which utilizes dynamically sized references to other record IDs and with ability
 * to use record units, meaning that a record may span two physical records in the store. This to keep store size
 * low and only have records that have big references occupy double amount of space. This format supports up to
 * 58-bit IDs, which is roughly 280 quadrillion. With that size the ID limits can be considered highlimit,
 * hence the format name. The IDs take up between 3-8B depending on the size of the ID where relative ID
 * references are used as often as possible. See {@link Reference}.
 *
 * For consistency, all formats have a one-byte header specifying:
 *
 * <ol>
 * <li>0x1: inUse [0=unused, 1=used]</li>
 * <li>0x2: record unit [0=single record, 1=multiple records]</li>
 * <li>0x4: record unit type [0=first, 1=consecutive]
 * <li>0x8 - 0x80 other flags for this record specific to each type</li>
 * </ol>
 *
 * NOTE to the rest of the flags is that a good use of them is to denote whether or not an ID reference is
 * null (-1) as to save 3B (smallest compressed size) by not writing a reference at all.
 *
 * For records that are the first out of multiple record units, then immediately following the header byte is
 * the reference (3-8B) to the secondary ID. After that the "statically sized" data and in the end the
 * dynamically sized data. The general thinking is that the break-off into the secondary record will happen in
 * the sequence of dynamically sized references and this will allow for crossing the record boundary
 * in between, but even in the middle of, references quite easily since the {@link DataAdapter}
 * works on byte-per-byte data.
 *
 * Assigning secondary record unit IDs is done outside of this format implementation, it is just assumed
 * that records that gets {@link #write(AbstractBaseRecord, PageCursor, int, PagedFile) written} have already
 * been assigned all required such data.
 *
 * Usually each records are written and read atomically, so this format requires additional logic to be able to
 * write and read multiple records together atomically. For writing then currently this is guarded by
 * higher level entity write locks and so the {@link PageCursor} can simply move from the first on to the second
 * record and continue writing. For reading, which is optimistic and may require retry, one additional
 * {@link PageCursor} needs to be acquired over the second record, checking {@link PageCursor#shouldRetry()}
 * on both and potentially re-reading the second or both until a consistent read was had.
 *
 * @param <RECORD> type of {@link AbstractBaseRecord}
 */
abstract class BaseHighLimitRecordFormat<RECORD extends AbstractBaseRecord>
        extends BaseOneByteHeaderRecordFormat<RECORD>
{
    static final long NULL = Record.NULL_REFERENCE.intValue();
    static final int HEADER_BIT_RECORD_UNIT = 0b0000_0010;
    static final int HEADER_BIT_FIRST_RECORD_UNIT = 0b0000_0100;
    // Default to community record format
    private final RecordIO<RECORD> recordIO; // = new RecordIO.CommunityRecordIO<>();

    protected BaseHighLimitRecordFormat( Function<StoreHeader,Integer> recordSize, int recordHeaderSize,
            RecordIO<RECORD> recordIO )
    {
        super( recordSize, recordHeaderSize, IN_USE_BIT );
        this.recordIO = recordIO;
    }

    @Override
    protected final void doRead( RECORD record, PageCursor primaryCursor, int recordSize, PagedFile storeFile,
            long headerByte, boolean inUse ) throws IOException
    {
        boolean doubleRecordUnit = has( headerByte, HEADER_BIT_RECORD_UNIT );
        if ( doubleRecordUnit )
        {
            boolean firstRecordUnit = has( headerByte, HEADER_BIT_FIRST_RECORD_UNIT );
            if ( !firstRecordUnit )
            {
                // This is a record unit and not even the first one, so you cannot go here directly and read it,
                // it may only be read as part of reading the primary unit.
                record.clear();
                // Return and try again
                return;
            } else {
                recordIO.read( record, primaryCursor, recordSize, storeFile,
                        ( readAdapter ) -> doReadInternal( record, primaryCursor, recordSize, headerByte, inUse,
                                readAdapter ) );
            }
        } else {
            doReadInternal( record, primaryCursor, recordSize, headerByte, inUse, PAGE_CURSOR_ADAPTER );
        }
    }

    protected abstract void doReadInternal( RECORD record, PageCursor cursor, int recordSize,
            long inUseByte, boolean inUse, DataAdapter<PageCursor> adapter );

    @Override
    protected final void doWrite( RECORD record, PageCursor primaryCursor, int recordSize, PagedFile storeFile )
            throws IOException
    {
        // Let the specific implementation provide the additional header bits and we'll provide the core format bits.
        byte headerByte = headerBits( record );
        assert (headerByte & 0x7) == 0 : "Format-specific header bits (" + headerByte +
                ") collides with format-generic header bits";
        headerByte = set( headerByte, IN_USE_BIT, record.inUse() );
        headerByte = set( headerByte, HEADER_BIT_RECORD_UNIT, record.requiresSecondaryUnit() );
        headerByte = set( headerByte, HEADER_BIT_FIRST_RECORD_UNIT, true );
        primaryCursor.putByte( headerByte );

        if ( record.requiresSecondaryUnit() )
        {
            doWriteInternal( record, primaryCursor,
                    recordIO.getWriteAdapter( record, primaryCursor, recordSize, storeFile ) );
        }
        else
        {
            doWriteInternal( record, primaryCursor, PAGE_CURSOR_ADAPTER );
        }
    }

    protected abstract void doWriteInternal( RECORD record, PageCursor cursor, DataAdapter<PageCursor> adapter )
            throws IOException;

    protected abstract byte headerBits( RECORD record );

    @Override
    public final void prepare( RECORD record, int recordSize, IdSequence idSequence )
    {
        assert record.inUse();
        int length = 1 + requiredDataLength( record );
        boolean requiresSecondaryUnit = length > recordSize;
        record.setRequiresSecondaryUnit( requiresSecondaryUnit );
        if ( record.requiresSecondaryUnit() && !record.hasSecondaryUnitId() )
        {
            // Allocate a new id at this point, but this is not the time to free this ID the the case where
            // this record doesn't need this secondary unit anymore... that needs to be done when applying to store.
            record.setSecondaryUnitId( idSequence.nextId() );
        }
    }

    /**
     * Required length of the data in the given record (without the header byte).
     *
     * @param record data to check how much space it would require.
     * @return length required to store the data in the given record.
     */
    protected abstract int requiredDataLength( RECORD record );

    protected static int length( long reference )
    {
        return Reference.length( reference );
    }

    protected static int length( long reference, long nullValue )
    {
        return reference == nullValue ? 0 : length( reference );
    }

    protected static long decode( PageCursor cursor, DataAdapter<PageCursor> adapter )
    {
        return Reference.decode( cursor, adapter );
    }

    protected static long decode( PageCursor cursor,
            DataAdapter<PageCursor> adapter, long headerByte, int headerBitMask, long nullValue )
    {
        return has( headerByte, headerBitMask ) ? decode( cursor, adapter ) : nullValue;
    }

    protected static void encode( PageCursor cursor, DataAdapter<PageCursor> adapter, long reference )
            throws IOException
    {
        Reference.encode( reference, cursor, adapter );
    }

    protected static void encode( PageCursor cursor, DataAdapter<PageCursor> adapter, long reference,
            long nullValue ) throws IOException
    {
        if ( reference != nullValue )
        {
            Reference.encode( reference, cursor, adapter );
        }
    }

    protected static byte set( byte header, int bitMask, long reference, long nullValue )
    {
        return set( header, bitMask, reference != nullValue );
    }
}
