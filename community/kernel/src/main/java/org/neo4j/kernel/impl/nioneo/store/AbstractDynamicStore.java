/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;

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
public abstract class AbstractDynamicStore extends CommonAbstractStore implements Store, RecordStore<DynamicRecord>
{
    public static abstract class Configuration
        extends CommonAbstractStore.Configuration
    {
        public static final GraphDatabaseSetting.BooleanSetting rebuild_idgenerators_fast = GraphDatabaseSettings.rebuild_idgenerators_fast;
    }

    private Config conf;
    private int blockSize;

    public AbstractDynamicStore( String fileName, Config conf, IdType idType,
                                 IdGeneratorFactory idGeneratorFactory, FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger)
    {
        super( fileName, conf, idType, idGeneratorFactory, fileSystemAbstraction, stringLogger );
        this.conf = conf;
    }

    @Override
    protected int getEffectiveRecordSize()
    {
        return getBlockSize();
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

    @Override
    protected void verifyFileSizeAndTruncate() throws IOException
    {
        int expectedVersionLength = UTF8.encode( buildTypeDescriptorAndVersion( getTypeDescriptor() ) ).length;
        long fileSize = getFileChannel().size();
        if ( (fileSize - expectedVersionLength) % blockSize != 0 && !isReadOnly() )
        {
            setStoreNotOk( new IllegalStateException( "Misaligned file size " + fileSize + " for " + this + ", expected version length " + expectedVersionLength ) );
        }
        if ( getStoreOk() && !isReadOnly() )
        {
            getFileChannel().truncate( fileSize - expectedVersionLength );
        }
    }

    @Override
    protected void readAndVerifyBlockSize() throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( 4 );
        getFileChannel().position( 0 );
        getFileChannel().read( buffer );
        buffer.flip();
        blockSize = buffer.getInt();
        if ( blockSize <= 0 )
        {
            throw new InvalidRecordException( "Illegal block size: " +
            blockSize + " in " + getStorageFileName() );
        }
    }

    /**
     * Returns the byte size of each block for this dynamic store
     *
     * @return The block size of this store
     */
    public int getBlockSize()
    {
        return blockSize;
    }

    /**
     * Returns next free block.
     *
     * @return The next free block
     * @throws IOException
     *             If capacity exceeded or closed id generator
     */
    public long nextBlockId()
    {
        return nextId();
    }

    /**
     * Makes a previously used block available again.
     *
     * @param blockId
     *            The id of the block to free
     * @throws IOException
     *             If id generator closed or illegal block id
     */
    public void freeBlockId( long blockId )
    {
        freeId( blockId );
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

    // (in_use+next high)(1 byte)+nr_of_bytes(3 bytes)+next_block(int)
    public static final int BLOCK_HEADER_SIZE = 1 + 3 + 4; // = 8

    public void updateRecord( DynamicRecord record )
    {
        long blockId = record.getId();
        if ( isInRecoveryMode() )
        {
            registerIdFromUpdateRecord( blockId );
        }
        PersistenceWindow window = acquireWindow( blockId, OperationType.WRITE );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( blockId );
            if ( record.inUse() )
            {
                long nextProp = record.getNextBlock();
                int nextModifier = nextProp == Record.NO_NEXT_BLOCK.intValue() ? 0
                        : (int) ( ( nextProp & 0xF00000000L ) >> 8 );
                nextModifier |= ( Record.IN_USE.byteValue() << 28 );

                /*
                 *
                 * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
                 * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
                 * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes
                 *
                 */
                int mostlyNrOfBytesInt = record.getLength();
                assert mostlyNrOfBytesInt < ( 1 << 24 ) - 1;

                mostlyNrOfBytesInt |= nextModifier;

                buffer.putInt( mostlyNrOfBytesInt ).putInt( (int) nextProp );
                if ( !record.isLight() )
                {
                    buffer.put( record.getData() );
                }
                else
                {
                    assert getHighId() != record.getId() + 1;
                }
            }
            else
            {
                buffer.put( Record.NOT_IN_USE.byteValue() );
                if ( !isInRecoveryMode() )
                {
                    freeBlockId( blockId );
                }
            }
        }
        finally
        {
            releaseWindow( window );
        }
    }

    @Override
    public void forceUpdateRecord( DynamicRecord record )
    {
        updateRecord( record );
    }

    protected Collection<DynamicRecord> allocateRecords( long startBlock,
        byte src[] )
    {
        assert getFileChannel() != null : "Store closed, null file channel";
        assert src != null : "Null src argument";
        List<DynamicRecord> recordList = new LinkedList<DynamicRecord>();
        long nextBlock = startBlock;
        int srcOffset = 0;
        int dataSize = getBlockSize() - BLOCK_HEADER_SIZE;
        do
        {
            DynamicRecord record = new DynamicRecord( nextBlock );
            record.setCreated();
            record.setInUse( true );
            if ( src.length - srcOffset > dataSize )
            {
                byte data[] = new byte[dataSize];
                System.arraycopy( src, srcOffset, data, 0, dataSize );
                record.setData( data );
                nextBlock = nextBlockId();
                record.setNextBlock( nextBlock );
                srcOffset += dataSize;
            }
            else
            {
                byte data[] = new byte[src.length - srcOffset];
                System.arraycopy( src, srcOffset, data, 0, data.length );
                record.setData( data );
                nextBlock = Record.NO_NEXT_BLOCK.intValue();
                record.setNextBlock( nextBlock );
            }
            recordList.add( record );
            assert !record.isLight();
            assert record.getData() != null;
        }
        while ( nextBlock != Record.NO_NEXT_BLOCK.intValue() );
        return recordList;
    }

    public Collection<DynamicRecord> getLightRecords( long startBlockId )
    {
        List<DynamicRecord> recordList = new LinkedList<DynamicRecord>();
        long blockId = startBlockId;
        while ( blockId != Record.NO_NEXT_BLOCK.intValue() )
        {
            PersistenceWindow window = acquireWindow( blockId,
                OperationType.READ );
            try
            {
                DynamicRecord record = getRecord( blockId, window, RecordLoad.CHECK );
                recordList.add( record );
                blockId = record.getNextBlock();
            }
            finally
            {
                releaseWindow( window );
            }
        }
        return recordList;
    }

    public void makeHeavy( DynamicRecord record )
    {
        long blockId = record.getId();
        PersistenceWindow window = acquireWindow( blockId, OperationType.READ );
        try
        {
            Buffer buf = window.getBuffer();
            // NOTE: skip of header in offset
            int offset = (int) (blockId-buf.position()) * getBlockSize() + BLOCK_HEADER_SIZE;
            buf.setOffset( offset );
            byte bytes[] = new byte[record.getLength()];
            buf.get( bytes );
            record.setData( bytes );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    protected boolean isRecordInUse( ByteBuffer buffer )
    {
        return ( ( buffer.get() & (byte) 0xF0 ) >> 4 ) == Record.IN_USE.byteValue();
    }

    private DynamicRecord getRecord( long blockId, PersistenceWindow window, RecordLoad load )
    {
        DynamicRecord record = new DynamicRecord( blockId );
        Buffer buffer = window.getOffsettedBuffer( blockId );

        /*
         *
         * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
         * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
         * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes
         *
         */
        long firstInteger = buffer.getUnsignedInt();

        int inUseByte = (int) ( ( firstInteger & 0xF0000000 ) >> 28 );
        boolean inUse = inUseByte == Record.IN_USE.intValue();
        if ( !inUse && load != RecordLoad.FORCE )
        {
            throw new InvalidRecordException( "DynamicRecord Not in use, blockId[" + blockId + "]" );
        }
        int dataSize = getBlockSize() - BLOCK_HEADER_SIZE;

        int nrOfBytes = (int) ( firstInteger & 0xFFFFFF );

        long nextBlock = buffer.getUnsignedInt();
        long nextModifier = ( firstInteger & 0xF000000L ) << 8;

        long longNextBlock = longFromIntAndMod( nextBlock, nextModifier );
        boolean readData = load != RecordLoad.CHECK;
        if ( longNextBlock != Record.NO_NEXT_BLOCK.intValue()
            && nrOfBytes < dataSize || nrOfBytes > dataSize )
        {
            readData = false;
            if ( load != RecordLoad.FORCE )
                throw new InvalidRecordException( "Next block set[" + nextBlock
                + "] current block illegal size[" + nrOfBytes + "/" + dataSize + "]" );
        }
        record.setInUse( inUse );
        record.setLength( nrOfBytes );
        record.setNextBlock( longNextBlock );
        if ( readData )
        {
            byte byteArrayElement[] = new byte[nrOfBytes];
            buffer.get( byteArrayElement );
            record.setData( byteArrayElement );
        }
        return record;
    }

    @Override
    public DynamicRecord getRecord( long id )
    {
        PersistenceWindow window = acquireWindow( id,
                OperationType.READ );
        try
        {
            return getRecord( id, window, RecordLoad.NORMAL );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    @Override
    public DynamicRecord forceGetRecord( long id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            return new DynamicRecord( id );
        }

        try
        {
            return getRecord( id, window, RecordLoad.FORCE );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    @Override
    public DynamicRecord forceGetRaw( long id )
    {
        return forceGetRecord( id );
    }

    public Collection<DynamicRecord> getRecords( long startBlockId )
    {
        List<DynamicRecord> recordList = new LinkedList<DynamicRecord>();
        long blockId = startBlockId;
        while ( blockId != Record.NO_NEXT_BLOCK.intValue() )
        {
            PersistenceWindow window = acquireWindow( blockId,
                OperationType.READ );
            try
            {
                DynamicRecord record = getRecord( blockId, window, RecordLoad.NORMAL );
                recordList.add( record );
                blockId = record.getNextBlock();
            }
            finally
            {
                releaseWindow( window );
            }
        }
        return recordList;
    }

    private long findHighIdBackwards() throws IOException
    {
        FileChannel fileChannel = getFileChannel();
        int recordSize = getBlockSize();
        long fileSize = fileChannel.size();
        long highId = fileSize / recordSize;
        ByteBuffer byteBuffer = ByteBuffer.allocate( 1 );
        for ( long i = highId; i > 0; i-- )
        {
            fileChannel.position( i * recordSize );
            if ( fileChannel.read( byteBuffer ) > 0 )
            {
                byteBuffer.flip();
                boolean isInUse = isRecordInUse( byteBuffer );
                byteBuffer.clear();
                if ( isInUse )
                {
                    return i;
                }
            }
        }
        return 0;
    }

    /**
     * Rebuilds the internal id generator keeping track of what blocks are free
     * or taken.
     *
     * @throws IOException
     *             If unable to rebuild the id generator
     */
    @Override
    protected void rebuildIdGenerator()
    {
        if ( getBlockSize() <= 0 )
        {
            throw new InvalidRecordException( "Illegal blockSize: " +
                getBlockSize() );
        }
        logger.fine( "Rebuilding id generator for[" + getStorageFileName()
            + "] ..." );
        closeIdGenerator();
        if ( fileSystemAbstraction.fileExists( getStorageFileName() + ".id" ) )
        {
            boolean success = fileSystemAbstraction.deleteFile( getStorageFileName() + ".id" );
            assert success;
        }
        createIdGenerator( getStorageFileName() + ".id" );
        openIdGenerator( false );
        setHighId( 1 ); // reserved first block containing blockSize
        FileChannel fileChannel = getFileChannel();
        long highId = 0;
        long defraggedCount = 0;
        try
        {
            long fileSize = fileChannel.size();
            boolean fullRebuild = true;

            if ( (boolean) conf.get( Configuration.rebuild_idgenerators_fast ) )
            {
                fullRebuild = false;
                highId = findHighIdBackwards();
            }

            ByteBuffer byteBuffer = ByteBuffer.wrap( new byte[1] );
            LinkedList<Long> freeIdList = new LinkedList<Long>();
            if ( fullRebuild )
            {
                for ( long i = 1; i * getBlockSize() < fileSize; i++ )
                {
                    fileChannel.position( i * getBlockSize() );
                    byteBuffer.clear();
                    fileChannel.read( byteBuffer );
                    byteBuffer.flip();
                    if ( !isRecordInUse( byteBuffer ) )
                    {
                        freeIdList.add( i );
                    }
                    else
                    {
                        highId = i;
                        setHighId( highId + 1 );
                        while ( !freeIdList.isEmpty() )
                        {
                            freeBlockId( freeIdList.removeFirst() );
                            defraggedCount++;
                        }
                    }
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                "Unable to rebuild id generator " + getStorageFileName(), e );
        }
        setHighId( highId + 1 );
        logger.fine( "[" + getStorageFileName() + "] high id=" + getHighId()
            + " (defragged=" + defraggedCount + ")" );
        if ( stringLogger != null )
        {
            stringLogger.logMessage( getStorageFileName() + " rebuild id generator, highId=" + getHighId() +
                    " defragged count=" + defraggedCount, true );
        }
        closeIdGenerator();
        openIdGenerator( false );
    }

//    @Override
//    protected void updateHighId()
//    {
//        try
//        {
//            long highId = getFileChannel().size() / getBlockSize();
//
//            if ( highId > getHighId() )
//            {
//                setHighId( highId );
//            }
//        }
//        catch ( IOException e )
//        {
//            throw new UnderlyingStorageException( e );
//        }
//    }

    @Override
    protected long figureOutHighestIdInUse()
    {
        try
        {
            return getFileChannel().size()/getBlockSize();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public String toString()
    {
        return super.toString() + "[blockSize:" + (getRecordSize()-getRecordHeaderSize()) + "]";
    }
}
