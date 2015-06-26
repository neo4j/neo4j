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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * An abstract representation of a dynamic store. The difference between a
 * normal {@link AbstractStore} and a <CODE>AbstractDynamicStore</CODE> is
 * that the size of a record/entry can be dynamic.
 * <p>
 * Instead of a fixed record this class uses blocks to store a record. If a
 * record size is greater than the block size the record will use one or more
 * blocks to store its data.
 * <p>
 * A dynamic store don't have a {@link IdGenerator} because the position of a
 * record can't be calculated just by knowing the id. Instead one should use a
 * {@link AbstractStore} and store the start block of the record located in the
 * dynamic store. Note: This class makes use of an id generator internally for
 * managing free and non free blocks.
 * <p>
 * Note, the first block of a dynamic store is reserved and contains information
 * about the store.
 */
public abstract class AbstractDynamicStore extends CommonAbstractStore<DynamicRecord>
        implements DynamicRecordAllocator
{
    public static final byte[] NO_DATA = new byte[0];
    // (in_use+next high)(1 byte)+nr_of_bytes(3 bytes)+next_block(int)
    public static final int RECORD_HEADER_SIZE = 1 + 3 + 4; // = 8

    private int recordSize;

    public AbstractDynamicStore(
            File fileName,
            Config conf,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            String typeDescriptor,
            int dataSizeFromConfiguration )
    {
        super( fileName, conf, idType, idGeneratorFactory, pageCache, logProvider, typeDescriptor );
        this.recordSize = dataSizeFromConfiguration + RECORD_HEADER_SIZE;
    }

    /**
     * Calculate the size of a dynamic record given the size of the data block.
     *
     * @param dataSize the size of the data block in bytes.
     * @return the size of a dynamic record.
     */
    public static int getRecordSize( int dataSize )
    {
        return dataSize + RECORD_HEADER_SIZE;
    }

    @Override
    protected ByteBuffer createHeaderRecord()
    {
        if ( recordSize < 1 || recordSize > 0xFFFF )
        {
            throw new IllegalArgumentException( "Illegal block size[" + recordSize + "], limit is 65535" );
        }
        return intHeaderData( recordSize );
    }

    @Override
    public long getNextRecordReference( DynamicRecord record )
    {
        return record.getNextBlock();
    }

    public static void allocateRecordsFromBytes(
            Collection<DynamicRecord> recordList, byte src[], Iterator<DynamicRecord> recordsToUseFirst,
            DynamicRecordAllocator dynamicRecordAllocator )
    {
        assert src != null : "Null src argument";
        DynamicRecord nextRecord = dynamicRecordAllocator.nextUsedRecordOrNew( recordsToUseFirst );
        int srcOffset = 0;
        int dataSize = dynamicRecordAllocator.dataSize();
        do
        {
            DynamicRecord record = nextRecord;
            record.setStartRecord( srcOffset == 0 );
            if ( src.length - srcOffset > dataSize )
            {
                byte data[] = new byte[dataSize];
                System.arraycopy( src, srcOffset, data, 0, dataSize );
                record.setData( data );
                nextRecord = dynamicRecordAllocator.nextUsedRecordOrNew( recordsToUseFirst );
                record.setNextBlock( nextRecord.getId() );
                srcOffset += dataSize;
            }
            else
            {
                byte data[] = new byte[src.length - srcOffset];
                System.arraycopy( src, srcOffset, data, 0, data.length );
                record.setData( data );
                nextRecord = null;
                record.setNextBlock( Record.NO_NEXT_BLOCK.intValue() );
            }
            recordList.add( record );
            assert record.getData() != null;
        }
        while ( nextRecord != null );
    }

    /**
     * @return a {@link ByteBuffer#slice() sliced} {@link ByteBuffer} wrapping {@code target} or,
     * if necessary a new larger {@code byte[]} and containing exactly all concatenated data read from records
     */
    public static ByteBuffer concatData( Collection<DynamicRecord> records, byte[] target )
    {
        int totalLength = 0;
        for ( DynamicRecord record : records )
        {
            totalLength += record.getLength();
        }

        if ( target.length < totalLength )
        {
            target = new byte[totalLength];
        }

        ByteBuffer buffer = ByteBuffer.wrap( target, 0, totalLength );
        for ( DynamicRecord record : records )
        {
            buffer.put( record.getData() );
        }
        buffer.position( 0 );
        return buffer;
    }

    /**
     * @return Pair&lt; header-in-first-record , all-other-bytes &gt;
     */
    public static Pair<byte[], byte[]> readFullByteArrayFromHeavyRecords(
            Iterable<DynamicRecord> records, PropertyType propertyType )
    {
        byte[] header = null;
        List<byte[]> byteList = new ArrayList<>();
        int totalSize = 0, i = 0;
        for ( DynamicRecord record : records )
        {
            int offset = 0;
            if ( i++ == 0 )
            {   // This is the first one, read out the header separately
                header = propertyType.readDynamicRecordHeader( record.getData() );
                offset = header.length;
            }

            byteList.add( record.getData() );
            totalSize += (record.getData().length - offset);
        }
        byte[] bArray = new byte[totalSize];
        assert header != null : "header should be non-null since records should not be empty";
        int sourceOffset = header.length;
        int offset = 0;
        for ( byte[] currentArray : byteList )
        {
            System.arraycopy( currentArray, sourceOffset, bArray, offset,
                    currentArray.length - sourceOffset );
            offset += (currentArray.length - sourceOffset);
            sourceOffset = 0;
        }
        return Pair.of( header, bArray );
    }

    @Override
    public DynamicRecord nextUsedRecordOrNew( Iterator<DynamicRecord> recordsToUseFirst )
    {
        DynamicRecord record;
        if ( recordsToUseFirst.hasNext() )
        {
            record = recordsToUseFirst.next();
            if ( !record.inUse() )
            {
                record.setCreated();
            }
        }
        else
        {
            record = new DynamicRecord( nextId() );
            record.setCreated();
        }
        record.setInUse( true );
        return record;
    }

    @Override
    public int dataSize()
    {
        return getRecordSize() - AbstractDynamicStore.RECORD_HEADER_SIZE;
    }

    @Override
    public int getRecordSize()
    {
        return recordSize;
    }

    @Override
    public int getRecordHeaderSize()
    {
        return RECORD_HEADER_SIZE;
    }

    /**
     * We reserve the first record, record 0, to contain an integer saying how big the record size of
     * this store is. Record size of a dynamic store can be specified at creation time, and will be
     * put here and read every time the store is loaded.
     */
    @Override
    public int getNumberOfReservedLowIds()
    {
        return 1;
    }

    @Override
    protected void readAndVerifyHeaderRecord() throws IOException
    {
        recordSize = getHeaderRecord();
    }

    @Override
    public DynamicRecord newRecord()
    {
        return new DynamicRecord( -1 );
    }

    @Override
    protected void writeRecord( PageCursor cursor, DynamicRecord record )
    {
        if ( record.inUse() )
        {
            long nextBlock = record.getNextBlock();
            int highByteInFirstInteger = nextBlock == Record.NO_NEXT_BLOCK.intValue() ? 0
                    : (int) ((nextBlock & 0xF00000000L) >> 8);
            highByteInFirstInteger |= (Record.IN_USE.byteValue() << 28);
            highByteInFirstInteger |= (record.isStartRecord() ? 0 : 1) << 31;

            /*
             * First 4b
             * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
             * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
             * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
             * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record
             *
             */
            int firstInteger = record.getLength();
            assert firstInteger < (1 << 24) - 1;

            firstInteger |= highByteInFirstInteger;

            cursor.putInt( firstInteger );
            cursor.putInt( (int) nextBlock );
            cursor.putBytes( record.getData() );
        }
        else
        {
            cursor.putByte( Record.NOT_IN_USE.byteValue() );
        }
    }

    public void allocateRecordsFromBytes( Collection<DynamicRecord> target, byte src[] )
    {
        allocateRecordsFromBytes( target, src, IteratorUtil.<DynamicRecord>emptyIterator(), this );
    }

    @Override
    protected void readRecord( PageCursor cursor, DynamicRecord record, RecordLoad mode )
    {
        /*
         * First 4b
         * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
         * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
         * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
         * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record
         *
         */
        long firstInteger = cursor.getUnsignedInt();
        boolean isStartRecord = (firstInteger & 0x80000000) == 0;
        boolean inUse = (firstInteger & 0x10000000) != 0;
        if ( mode.shouldLoad( inUse ) )
        {
            int dataSize = getRecordSize() - AbstractDynamicStore.RECORD_HEADER_SIZE;
            int nrOfBytes = (int) (firstInteger & 0xFFFFFF);

            /*
             * Pointer to next block 4b (low bits of the pointer)
             */
            long nextBlock = cursor.getUnsignedInt();
            long nextModifier = (firstInteger & 0xF000000L) << 8;

            long longNextBlock = CommonAbstractStore.longFromIntAndMod( nextBlock, nextModifier );
            record.initialize( inUse, isStartRecord, longNextBlock, -1, nrOfBytes );
            if ( longNextBlock != Record.NO_NEXT_BLOCK.intValue()
                    && nrOfBytes < dataSize || nrOfBytes > dataSize )
            {
                int blockDataSize = getRecordSize() - getRecordHeaderSize();
                mode.report( format( "Next block set[%d] current block illegal size[%d/%d]",
                        record.getNextBlock(), record.getLength(), blockDataSize ) );
            }

            if ( record.getLength() == 0 ) // don't go though the trouble of acquiring the window if we would read nothing
            {
                record.setData( NO_DATA );
                return;
            }

            int len = record.getLength();
            byte[] data = record.getData();
            if ( data == null || data.length != len )
            {
                data = new byte[len];
            }
            cursor.getBytes( data );
            record.setData( data );
        }
    }

    @Override
    protected boolean isInUse( byte inUseByte )
    {
        return ((inUseByte & (byte) 0x10) >> 4) == Record.IN_USE.byteValue();
    }

    @Override
    public String toString()
    {
        return super.toString() + "[fileName:" + storageFileName.getName() +
                ", blockSize:" + (getRecordSize() - getRecordHeaderSize()) + "]";
    }

    public Pair<byte[]/*header in the first record*/, byte[]/*all other bytes*/> readFullByteArray(
            Iterable<DynamicRecord> records, PropertyType propertyType )
    {
        for ( DynamicRecord record : records )
        {
            ensureHeavy( record );
        }

        return readFullByteArrayFromHeavyRecords( records, propertyType );
    }
}
