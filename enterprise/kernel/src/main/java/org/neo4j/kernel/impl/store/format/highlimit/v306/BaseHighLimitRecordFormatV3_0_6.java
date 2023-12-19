/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.store.format.highlimit.v306;

import java.io.IOException;
import java.util.function.Function;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.impl.CompositePageCursor;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.highlimit.Reference;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.offsetForId;
import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.pageIdForRecord;

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
 * <li>0x4: record unit type [1=first, 0=consecutive]
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
 * in between, but even in the middle of, references quite easily since the {@link CompositePageCursor}
 * handles the transition seamlessly.
 *
 * Assigning secondary record unit IDs is done outside of this format implementation, it is just assumed
 * that records that gets {@link RecordFormat#write(AbstractBaseRecord, PageCursor, int) written} have already
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
abstract class BaseHighLimitRecordFormatV3_0_6<RECORD extends AbstractBaseRecord>
        extends BaseOneByteHeaderRecordFormat<RECORD>
{
    static final int HEADER_BYTE = Byte.BYTES;

    static final long NULL = Record.NULL_REFERENCE.intValue();
    static final int HEADER_BIT_RECORD_UNIT = 0b0000_0010;
    static final int HEADER_BIT_FIRST_RECORD_UNIT = 0b0000_0100;
    static final int HEADER_BIT_FIXED_REFERENCE = 0b0000_0100;

    protected BaseHighLimitRecordFormatV3_0_6( Function<StoreHeader,Integer> recordSize, int recordHeaderSize )
    {
        super( recordSize, recordHeaderSize, IN_USE_BIT, HighLimitV3_0_6.DEFAULT_MAXIMUM_BITS_PER_ID );
    }

    @Override
    public void read( RECORD record, PageCursor primaryCursor, RecordLoad mode, int recordSize )
            throws IOException
    {
        int primaryStartOffset = primaryCursor.getOffset();
        byte headerByte = primaryCursor.getByte();
        boolean inUse = isInUse( headerByte );
        boolean doubleRecordUnit = has( headerByte, HEADER_BIT_RECORD_UNIT );
        record.setUseFixedReferences( false );
        if ( doubleRecordUnit )
        {
            boolean firstRecordUnit = has( headerByte, HEADER_BIT_FIRST_RECORD_UNIT );
            if ( !firstRecordUnit )
            {
                // This is a record unit and not even the first one, so you cannot go here directly and read it,
                // it may only be read as part of reading the primary unit.
                record.clear();
                // Return and try again
                primaryCursor.setCursorException(
                        "Expected record to be the first unit in the chain, but record header says it's not" );
                return;
            }

            // This is a record that is split into multiple record units. We need a bit more clever
            // data structures here. For the time being this means instantiating one object,
            // but the trade-off is a great reduction in complexity.
            long secondaryId = Reference.decode( primaryCursor );
            long pageId = pageIdForRecord( secondaryId, primaryCursor.getCurrentPageSize(), recordSize );
            int offset = offsetForId( secondaryId, primaryCursor.getCurrentPageSize(), recordSize );
            PageCursor secondaryCursor = primaryCursor.openLinkedCursor( pageId );
            if ( (!secondaryCursor.next()) | offset < 0 )
            {
                // We must have made an inconsistent read of the secondary record unit reference.
                // No point in trying to read this.
                record.clear();
                primaryCursor.setCursorException( illegalSecondaryReferenceMessage( pageId ) );
                return;
            }
            secondaryCursor.setOffset( offset + HEADER_BYTE);
            int primarySize = recordSize - (primaryCursor.getOffset() - primaryStartOffset);
            // We *could* sanity check the secondary record header byte here, but we won't. If it is wrong, then we most
            // likely did an inconsistent read, in which case we'll just retry. Otherwise, if the header byte is wrong,
            // then there is little we can do about it here, since we are not allowed to throw exceptions.

            int secondarySize = recordSize - HEADER_BYTE;
            PageCursor composite = CompositePageCursor.compose(
                    primaryCursor, primarySize, secondaryCursor, secondarySize );
            doReadInternal( record, composite, recordSize, headerByte, inUse );
            record.setSecondaryUnitId( secondaryId );
        }
        else
        {
            record.setUseFixedReferences( isUseFixedReferences( headerByte ) );
            doReadInternal( record, primaryCursor, recordSize, headerByte, inUse );
        }
    }

    private boolean isUseFixedReferences( byte headerByte )
    {
        return !has( headerByte, HEADER_BIT_FIXED_REFERENCE );
    }

    private String illegalSecondaryReferenceMessage( long secondaryId )
    {
        return "Illegal secondary record reference: " + secondaryId;
    }

    protected abstract void doReadInternal(
            RECORD record, PageCursor cursor, int recordSize, long inUseByte, boolean inUse );

    @Override
    public void write( RECORD record, PageCursor primaryCursor, int recordSize )
            throws IOException
    {
        if ( record.inUse() )
        {
            // Let the specific implementation provide the additional header bits and we'll provide the core format bits.
            byte headerByte = headerBits( record );
            assert (headerByte & 0x7) == 0 : "Format-specific header bits (" + headerByte +
                                             ") collides with format-generic header bits";
            headerByte = set( headerByte, IN_USE_BIT, record.inUse() );
            headerByte = set( headerByte, HEADER_BIT_RECORD_UNIT, record.requiresSecondaryUnit() );
            if ( record.requiresSecondaryUnit() )
            {
                headerByte = set( headerByte, HEADER_BIT_FIRST_RECORD_UNIT, true );
            }
            else
            {
                headerByte = set( headerByte, HEADER_BIT_FIXED_REFERENCE, !record.isUseFixedReferences() );
            }
            primaryCursor.putByte( headerByte );

            if ( record.requiresSecondaryUnit() )
            {
                // Write using the normal adapter since the first reference we write cannot really overflow
                // into the secondary record
                long secondaryUnitId = record.getSecondaryUnitId();
                long pageId = pageIdForRecord( secondaryUnitId, primaryCursor.getCurrentPageSize(), recordSize );
                int offset = offsetForId( secondaryUnitId, primaryCursor.getCurrentPageSize(), recordSize );
                PageCursor secondaryCursor = primaryCursor.openLinkedCursor( pageId );
                if ( !secondaryCursor.next() )
                {
                    // We are not allowed to write this much data to the file, apparently.
                    record.clear();
                    return;
                }
                secondaryCursor.setOffset( offset );
                secondaryCursor.putByte( (byte) (IN_USE_BIT | HEADER_BIT_RECORD_UNIT) );
                int recordSizeWithoutHeader = recordSize - HEADER_BYTE;
                PageCursor composite = CompositePageCursor.compose(
                        primaryCursor, recordSizeWithoutHeader, secondaryCursor, recordSizeWithoutHeader );

                Reference.encode( secondaryUnitId, composite );
                doWriteInternal( record, composite );
            }
            else
            {
                doWriteInternal( record, primaryCursor );
            }
        }
        else
        {
            markAsUnused( primaryCursor, record, recordSize );
        }
    }

    /*
     * Use this instead of {@link #markFirstByteAsUnused(PageCursor)} to mark both record units,
     * if record has a reference to a secondary unit.
     */
    protected void markAsUnused( PageCursor cursor, RECORD record, int recordSize )
            throws IOException
    {
        markAsUnused( cursor );
        if ( record.hasSecondaryUnitId() )
        {
            long secondaryUnitId = record.getSecondaryUnitId();
            long pageIdForSecondaryRecord = pageIdForRecord( secondaryUnitId, cursor.getCurrentPageSize(), recordSize );
            int offsetForSecondaryId = offsetForId( secondaryUnitId, cursor.getCurrentPageSize(), recordSize );
            if ( !cursor.next( pageIdForSecondaryRecord ) )
            {
                throw new UnderlyingStorageException( "Couldn't move to secondary page " + pageIdForSecondaryRecord );
            }
            cursor.setOffset( offsetForSecondaryId );
            markAsUnused( cursor );
        }
    }

    protected abstract void doWriteInternal( RECORD record, PageCursor cursor );

    protected abstract byte headerBits( RECORD record );

    @Override
    public final void prepare( RECORD record, int recordSize, IdSequence idSequence )
    {
        if ( record.inUse() )
        {
            record.setUseFixedReferences( canUseFixedReferences( record, recordSize ));
            if ( !record.isUseFixedReferences() )
            {
                int requiredLength = HEADER_BYTE + requiredDataLength( record );
                boolean requiresSecondaryUnit = requiredLength > recordSize;
                record.setRequiresSecondaryUnit( requiresSecondaryUnit );
                if ( record.requiresSecondaryUnit() && !record.hasSecondaryUnitId() )
                {
                    // Allocate a new id at this point, but this is not the time to free this ID the the case where
                    // this record doesn't need this secondary unit anymore... that needs to be done when applying to store.
                    record.setSecondaryUnitId( idSequence.nextId() );
                }
            }
        }
    }

    protected abstract boolean canUseFixedReferences( RECORD record, int recordSize );

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

    protected static long decodeCompressedReference( PageCursor cursor )
    {
        return Reference.decode( cursor );
    }

    protected static long decodeCompressedReference( PageCursor cursor, long headerByte, int headerBitMask, long nullValue )
    {
        return has( headerByte, headerBitMask ) ? decodeCompressedReference( cursor ) : nullValue;
    }

    protected static void encode( PageCursor cursor, long reference )
    {
        Reference.encode( reference, cursor );
    }

    protected static void encode( PageCursor cursor, long reference, long nullValue )
    {
        if ( reference != nullValue )
        {
            Reference.encode( reference, cursor );
        }
    }

    protected static byte set( byte header, int bitMask, long reference, long nullValue )
    {
        return set( header, bitMask, reference != nullValue );
    }
}
