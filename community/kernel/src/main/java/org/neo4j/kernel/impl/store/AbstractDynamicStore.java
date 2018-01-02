/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.LinkedList;
import java.util.List;

import org.neo4j.cursor.GenericCursor;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.logging.LogProvider;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

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
public abstract class AbstractDynamicStore extends CommonAbstractStore implements RecordStore<DynamicRecord>,
        DynamicBlockSize, DynamicRecordAllocator
{
    private static final byte[] NO_DATA = new byte[0];
    // (in_use+next high)(1 byte)+nr_of_bytes(3 bytes)+next_block(int)
    public static final int BLOCK_HEADER_SIZE = 1 + 3 + 4; // = 8

    private final int blockSizeFromConfiguration;
    private int blockSize;

    public AbstractDynamicStore(
            File fileName,
            Config conf,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            int blockSizeFromConfiguration )
    {
        super( fileName, conf, idType, idGeneratorFactory, pageCache, logProvider );
        this.blockSizeFromConfiguration = blockSizeFromConfiguration;
    }

    /**
     * Calculate the size of a dynamic record given the size of the data block.
     *
     * @param dataSize the size of the data block in bytes.
     * @return the size of a dynamic record.
     */
    public static int getRecordSize( int dataSize )
    {
        return dataSize + BLOCK_HEADER_SIZE;
    }

    @Override
    protected void initialiseNewStoreFile( PagedFile file ) throws IOException
    {
        int blockSize = blockSizeFromConfiguration;
        if ( blockSize < 1 || blockSize > 0xFFFF )
        {
            throw new IllegalArgumentException( "Illegal block size[" + blockSize + "], limit is 65535" );
        }

        blockSize += BLOCK_HEADER_SIZE;

        try ( PageCursor pageCursor = file.io( 0, PagedFile.PF_EXCLUSIVE_LOCK ) )
        {
            if ( pageCursor.next() )
            {
                do
                {
                    pageCursor.putInt( blockSize );
                }
                while ( pageCursor.shouldRetry() );
            }
        }

        File idFileName = new File( storageFileName.getPath() + ".id" );
        idGeneratorFactory.create( idFileName, 0, true );
        // TODO highestIdInUse = 0 works now, but not when slave can create store files.
        IdGenerator idGenerator = idGeneratorFactory.open( idFileName, idType, 0 );
        idGenerator.nextId(); // reserve first for blockSize
        idGenerator.close();
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
            assert !record.isLight();
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
        assert header != null :
                "header should be non-null since records should not be empty: " + Iterables.toString( records, ", " );
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
        return getBlockSize() - AbstractDynamicStore.BLOCK_HEADER_SIZE;
    }

    @Override
    public int getRecordSize()
    {
        return getBlockSize();
    }

    @Override
    public int getRecordHeaderSize()
    {
        return BLOCK_HEADER_SIZE;
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
    protected void readAndVerifyBlockSize() throws IOException
    {
        blockSize = getHeaderRecord();
    }

    /**
     * Returns the byte size of each block for this dynamic store
     *
     * @return The block size of this store
     */
    @Override
    public int getBlockSize()
    {
        return blockSize;
    }

    @Override
    public void updateRecord( DynamicRecord record )
    {
        long blockId = record.getId();
        long pageId = pageIdForRecord( blockId );
        try ( PageCursor cursor = storeFile.io( pageId, PF_EXCLUSIVE_LOCK ) )
        {
            if ( cursor.next() )
            {
                do
                {
                    writeRecord( cursor, record );
                }
                while ( cursor.shouldRetry() );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    // [next][type][data]

    private void writeRecord( PageCursor cursor, DynamicRecord record )
    {
        long recordId = record.getId();
        int offset = offsetForId( recordId );
        cursor.setOffset( offset );
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
            if ( !record.isLight() )
            {
                cursor.putBytes( record.getData() );
            }
        }
        else
        {
            cursor.putByte( Record.NOT_IN_USE.byteValue() );
            freeId( recordId );
        }
    }

    @Override
    public void forceUpdateRecord( DynamicRecord record )
    {
        updateRecord( record );
    }

    public void allocateRecordsFromBytes( Collection<DynamicRecord> target, byte src[] )
    {
        allocateRecordsFromBytes( target, src, IteratorUtil.<DynamicRecord>emptyIterator(), this );
    }

    public Collection<DynamicRecord> getLightRecords( long startBlockId )
    {
        return getRecords( startBlockId, false );
    }

    private Collection<DynamicRecord> getRecords( long startBlockId, boolean readBothHeaderAndData )
    {
        // TODO we should instead be passed in a consumer of records, so we don't have to spend memory building up
        // this list
        List<DynamicRecord> recordList = new LinkedList<>();
        long blockId = startBlockId;
        int noNextBlock = Record.NO_NEXT_BLOCK.intValue();

        try ( PageCursor cursor = storeFile.io( 0, PF_SHARED_LOCK ) )
        {
            while ( blockId != noNextBlock && cursor.next( pageIdForRecord( blockId ) ) )
            {
                DynamicRecord record = new DynamicRecord( blockId );
                HeaderReadResult headerReadResult;
                do
                {
                    cursor.setOffset( offsetForId( blockId ) );
                    headerReadResult = readRecordHeader( cursor, record, false );
                    if ( headerReadResult == HeaderReadResult.DATA && readBothHeaderAndData )
                    {
                        readRecordData( cursor, record );
                    }
                }
                while ( cursor.shouldRetry() );

                checkForInUse( headerReadResult, record );
                checkForIllegalSize( headerReadResult, record );
                recordList.add( record );
                blockId = record.getNextBlock();
            }
            return recordList;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    public DynamicRecordCursor newDynamicRecordCursor()
    {
        return new DynamicRecordCursor();
    }

    DynamicRecordCursor getRecordsCursor( long startBlockId )
    {
        return getRecordsCursor( startBlockId, newDynamicRecordCursor() );
    }

    public DynamicRecordCursor getRecordsCursor( long startBlockId, DynamicRecordCursor dynamicRecordCursor )
    {
        try
        {
            PageCursor cursor = storeFile.io( 0, PF_SHARED_LOCK );
            dynamicRecordCursor.init( startBlockId, cursor );
            return dynamicRecordCursor;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void checkForInUse( HeaderReadResult headerReadResult, DynamicRecord record )
    {
        if ( headerReadResult == HeaderReadResult.NOT_IN_USE )
        {
            throw new InvalidRecordException( "DynamicRecord not in use, blockId[" + record.getId() + "]" );
        }
    }

    private void checkForIllegalSize( HeaderReadResult headerReadResult, DynamicRecord record )
    {
        if ( headerReadResult == HeaderReadResult.ILLEGAL_SIZE )
        {
            int dataSize = getBlockSize() - AbstractDynamicStore.BLOCK_HEADER_SIZE;
            throw new InvalidRecordException( "Next block set[" + record.getNextBlock()
                    + "] current block illegal size[" + record.getLength() + "/" + dataSize +
                    "]" );
        }
    }

    /**
     * Reads data from the cursor into the given record, and returns one of the signals specified above.
     */
    private HeaderReadResult readRecordHeader( PageCursor cursor, DynamicRecord record, boolean force )
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
        long maskedInteger = firstInteger & ~0x80000000;
        int highNibbleInMaskedInteger = (int) ((maskedInteger) >> 28);
        boolean inUse = highNibbleInMaskedInteger == Record.IN_USE.intValue();
        if ( !inUse && !force )
        {
            return HeaderReadResult.NOT_IN_USE;
        }
        int dataSize = getBlockSize() - AbstractDynamicStore.BLOCK_HEADER_SIZE;

        int nrOfBytes = (int) (firstInteger & 0xFFFFFF);

        /*
         * Pointer to next block 4b (low bits of the pointer)
         */
        long nextBlock = cursor.getUnsignedInt();
        long nextModifier = (firstInteger & 0xF000000L) << 8;

        long longNextBlock = CommonAbstractStore.longFromIntAndMod( nextBlock, nextModifier );
        boolean hasDataToRead = true;
        record.setInUse( inUse );
        record.setStartRecord( isStartRecord );
        record.setLength( nrOfBytes );
        record.setNextBlock( longNextBlock );
        if ( longNextBlock != Record.NO_NEXT_BLOCK.intValue()
                && nrOfBytes < dataSize || nrOfBytes > dataSize )
        {
            hasDataToRead = false;
            if ( !force )
            {
                return HeaderReadResult.ILLEGAL_SIZE;
            }
        }
        return hasDataToRead ? HeaderReadResult.DATA : HeaderReadResult.NO_DATA;
    }

    private void readRecordData( PageCursor cursor, DynamicRecord record )
    {
        int len = record.getLength();
        byte[] data = record.getData();
        if ( data == null || data.length != len )
        {
            data = new byte[len];
        }
        cursor.getBytes( data );
        record.setData( data );
    }

    public void ensureHeavy( DynamicRecord record )
    {
        if ( !record.isLight() )
        {
            return;
        }
        if ( record.getLength() == 0 ) // don't go though the trouble of acquiring the window if we would read nothing
        {
            record.setData( NO_DATA );
            return;
        }

        long pageId = pageIdForRecord( record.getId() );
        try ( PageCursor cursor = storeFile.io( pageId, PF_SHARED_LOCK ) )
        {
            if ( cursor.next() )
            {
                int offset = offsetForId( record.getId() );
                do
                {
                    // Add the BLOCK_HEADER_SIZE to the offset, since we don't read it
                    cursor.setOffset( offset + BLOCK_HEADER_SIZE );
                    readRecordData( cursor, record );
                }
                while ( cursor.shouldRetry() );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public DynamicRecord getRecord( long id )
    {
        DynamicRecord record = new DynamicRecord( id );
        long pageId = pageIdForRecord( id );
        try ( PageCursor cursor = storeFile.io( pageId, PF_SHARED_LOCK ) )
        {
            HeaderReadResult headerReadResult = HeaderReadResult.NOT_IN_USE;
            if ( cursor.next() )
            {
                int offset = offsetForId( record.getId() );
                do
                {
                    cursor.setOffset( offset );
                    headerReadResult = readRecordHeader( cursor, record, false );
                    if ( headerReadResult == HeaderReadResult.DATA )
                    {
                        readRecordData( cursor, record );
                    }
                }
                while ( cursor.shouldRetry() );
            }

            checkForInUse( headerReadResult, record );
            checkForIllegalSize( headerReadResult, record );
            return record;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public DynamicRecord forceGetRecord( long id )
    {
        DynamicRecord record = new DynamicRecord( id );
        long pageId = pageIdForRecord( id );
        try ( PageCursor cursor = storeFile.io( pageId, PF_SHARED_LOCK ) )
        {
            if ( cursor.next() )
            {
                int offset = offsetForId( record.getId() );
                HeaderReadResult headerReadResult;
                do
                {
                    cursor.setOffset( offset );
                    headerReadResult = readRecordHeader( cursor, record, true );
                    if ( headerReadResult == HeaderReadResult.DATA )
                    {
                        readRecordData( cursor, record );
                    }
                }
                while ( cursor.shouldRetry() );
                checkForIllegalSize( headerReadResult, record );
            }
            return record;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public Collection<DynamicRecord> getRecords( long startBlockId )
    {
        return getRecords( startBlockId, true );
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

    public class DynamicRecordCursor extends GenericCursor<DynamicRecord>
    {
        private PageCursor cursor;
        long blockId;
        int noNextBlock;

        private final DynamicRecord record = new DynamicRecord( blockId );

        public void init( long startBlockId, PageCursor cursor )
        {
            this.cursor = cursor;
            blockId = startBlockId;
            noNextBlock = Record.NO_NEXT_BLOCK.intValue();
        }

        @Override
        public boolean next()
        {
            try
            {
                if ( blockId != noNextBlock && cursor.next( pageIdForRecord( blockId ) ) )
                {
                    record.setId( blockId );

                    HeaderReadResult headerReadResult;
                    do
                    {
                        cursor.setOffset( offsetForId( blockId ) );
                        headerReadResult = readRecordHeader( cursor, record, true );
                        if ( headerReadResult == HeaderReadResult.DATA )
                        {
                            readRecordData( cursor, record );
                        }
                    }
                    while ( cursor.shouldRetry() );

                    checkForIllegalSize( headerReadResult, record );
                    current = record;
                    blockId = record.getNextBlock();
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }

        @Override
        public void close()
        {
            cursor.close();
            cursor = null;
        }
    }

    private enum HeaderReadResult
    {
        DATA, NO_DATA, NOT_IN_USE, ILLEGAL_SIZE
    }
}
