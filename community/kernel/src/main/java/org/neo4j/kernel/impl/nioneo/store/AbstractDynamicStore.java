/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
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
public abstract class AbstractDynamicStore extends CommonAbstractStore
{
    /**
     * Creates a new empty store. A factory method returning an implementation
     * should make use of this method to initialize an empty store. Block size
     * must be greater than zero. Not that the first block will be marked as
     * reserved (contains info about the block size). There will be an overhead
     * for each block of <CODE>13</CODE> bytes.
     * <p>
     * This method will create a empty store with descriptor returned by the
     * {@link #getTypeAndVersionDescriptor()}. The internal id generator used
     * by this store will also be created.
     * 
     * @param fileName
     *            The file name of the store that will be created
     * @param blockSize
     *            The number of bytes for each block
     * @param typeAndVersionDescriptor
     *            The type and version descriptor that identifies this store
     * 
     * @throws IOException
     *             If fileName is null or if file exists or illegal block size
     */
    protected static void createEmptyStore( String fileName, int baseBlockSize,
        String typeAndVersionDescriptor, IdGeneratorFactory idGeneratorFactory, IdType idType )
    {
        int blockSize = baseBlockSize;
        // sanity checks
        if ( fileName == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        File file = new File( fileName );
        if ( file.exists() )
        {
            throw new IllegalStateException( "Can't create store[" + fileName
                + "], file already exists" );
        }
        if ( blockSize < 1 )
        {
            throw new IllegalArgumentException( "Illegal block size["
                + blockSize + "]" );
        }
        if ( blockSize > 0xFFFF )
        {
            throw new IllegalArgumentException( "Illegal block size[" + blockSize + "], limit is 65535" );
        }
        blockSize += BLOCK_HEADER_SIZE;

        // write the header
        try
        {
            FileChannel channel = new FileOutputStream( fileName ).getChannel();
            int endHeaderSize = blockSize
                + UTF8.encode( typeAndVersionDescriptor ).length;
            ByteBuffer buffer = ByteBuffer.allocate( endHeaderSize );
            buffer.putInt( blockSize );
            buffer.position( endHeaderSize - typeAndVersionDescriptor.length() );
            buffer.put( UTF8.encode( typeAndVersionDescriptor ) ).flip();
            channel.write( buffer );
            channel.force( false );
            channel.close();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to create store "
                + fileName, e );
        }
        idGeneratorFactory.create( fileName + ".id" );
        // TODO highestIdInUse = 0 works now, but not when slave can create store files.
        IdGenerator idGenerator = idGeneratorFactory.open( fileName + ".id", 1, idType, 0 );
        idGenerator.nextId(); // reserv first for blockSize
        idGenerator.close();
    }

    private int blockSize;

    public AbstractDynamicStore( String fileName, Map<?,?> config, IdType idType )
    {
        super( fileName, config, idType );
    }

//    public AbstractDynamicStore( String fileName )
//    {
//        super( fileName );
//    }

    /**
     * Loads this store validating version and id generator. Also the block size
     * is loaded (contained in first block)
     */
    protected void loadStorage()
    {
        try
        {
            long fileSize = getFileChannel().size();
            String expectedVersion = getTypeAndVersionDescriptor();
            byte version[] = new byte[UTF8.encode( expectedVersion ).length];
            ByteBuffer buffer = ByteBuffer.wrap( version );
            getFileChannel().position( fileSize - version.length );
            getFileChannel().read( buffer );
            buffer = ByteBuffer.allocate( 4 );
            getFileChannel().position( 0 );
            getFileChannel().read( buffer );
            buffer.flip();
            blockSize = buffer.getInt();
            if ( blockSize <= 0 )
            {
                throw new InvalidRecordException( "Illegal block size: " + 
                    blockSize + " in " + getStorageFileName() );
            }
            if ( !expectedVersion.equals( UTF8.decode( version ) ) )
            {
                if ( !versionFound( UTF8.decode( version ) ) && !isReadOnly() )
                {
                    setStoreNotOk();
                }
            }
            if ( (fileSize - version.length) % blockSize != 0 && !isReadOnly() )
            {
                setStoreNotOk();
            }
            if ( getStoreOk() && !isReadOnly() )
            {
                getFileChannel().truncate( fileSize - version.length );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to load storage "
                + getStorageFileName(), e );
        }
        try
        {
            if ( !isReadOnly() || isBackupSlave() )
            {
                openIdGenerator();
            }
            else
            {
                openReadOnlyIdGenerator( getBlockSize() );
            }
        }
        catch ( InvalidIdGeneratorException e )
        {
            setStoreNotOk();
        }
        finally 
        {
            if ( !getStoreOk() )
            {
                if ( getConfig() != null )
                {
                    String storeDir = (String) getConfig().get( "store_dir" );
                    StringLogger msgLog = StringLogger.getLogger( storeDir );
                    msgLog.logMessage( getStorageFileName() + " non clean shutdown detected", true );
                }
            }
        }
        
        setWindowPool( new PersistenceWindowPool( getStorageFileName(),
            getBlockSize(), getFileChannel(), getMappedMem(), 
            getIfMemoryMapped(), isReadOnly() && !isBackupSlave() ) );
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

    // in_use(byte)+prev_block(int)+nr_of_bytes(int)+next_block(int)
    protected static final int BLOCK_HEADER_SIZE = 1 + 4 + 4 + 4;

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
                long prevProp = record.getPrevBlock();
                short prevModifier = prevProp == Record.NO_NEXT_BLOCK.intValue() ? 0 : (short)((prevProp & 0xF00000000L) >> 28);
                
                long nextProp = record.getNextBlock();
                int nextModifier = nextProp == Record.NO_NEXT_BLOCK.intValue() ? 0 : (int)((nextProp & 0xF00000000L) >> 8);
                
                // [    ,   x] in use
                // [xxxx,    ] high prev block bits
                short inUseUnsignedByte = (short)((Record.IN_USE.byteValue() | prevModifier));
                
                // [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes
                // [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
                int nrOfBytesInt = record.getLength();
                nrOfBytesInt |= nextModifier;
                
                assert record.getId() != record.getPrevBlock();
                buffer.put( (byte)inUseUnsignedByte ).putInt( (int)prevProp ).putInt( nrOfBytesInt )
                    .putInt( (int)nextProp );
                if ( !record.isLight() )
                {
                    if ( !record.isCharData() )
                    {
                        buffer.put( record.getData() );
                    }
                    else
                    {
                        buffer.put( record.getDataAsChar() );
                    }
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

    protected Collection<DynamicRecord> allocateRecords( long startBlock,
        byte src[] )
    {
        assert getFileChannel() != null : "Store closed, null file channel";
        assert src != null : "Null src argument";
        List<DynamicRecord> recordList = new LinkedList<DynamicRecord>();
        long nextBlock = startBlock;
        long prevBlock = Record.NO_PREV_BLOCK.intValue();
        int srcOffset = 0;
        int dataSize = getBlockSize() - BLOCK_HEADER_SIZE;
        do
        {
            DynamicRecord record = new DynamicRecord( nextBlock );
            record.setCreated();
            record.setInUse( true );
            assert prevBlock != nextBlock;
            record.setPrevBlock( prevBlock );
            if ( src.length - srcOffset > dataSize )
            {
                byte data[] = new byte[dataSize];
                System.arraycopy( src, srcOffset, data, 0, dataSize );
                record.setData( data );
                prevBlock = nextBlock;
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
        }
        while ( nextBlock != Record.NO_NEXT_BLOCK.intValue() );
        return recordList;
    }

    public Collection<DynamicRecord> allocateRecords( long startBlock,
        char src[] )
    {
        assert getFileChannel() != null : "Store closed, null file channel";
        assert src != null : "Null src argument";
        List<DynamicRecord> recordList = new LinkedList<DynamicRecord>();
        long nextBlock = startBlock;
        long prevBlock = Record.NO_PREV_BLOCK.intValue();
        int srcOffset = 0;
        int dataSize = getBlockSize() - BLOCK_HEADER_SIZE;
        do
        {
            DynamicRecord record = new DynamicRecord( nextBlock );
            record.setCreated();
            record.setInUse( true );
            assert prevBlock != nextBlock;
            record.setPrevBlock( prevBlock );
            if ( (src.length - srcOffset) * 2 > dataSize )
            {
                byte data[] = new byte[dataSize];
                CharBuffer charBuf = ByteBuffer.wrap( data ).asCharBuffer();
                charBuf.put( src, srcOffset, dataSize / 2 );
                record.setData( data );
                prevBlock = nextBlock;
                nextBlock = nextBlockId();
                record.setNextBlock( nextBlock );
                srcOffset += dataSize / 2;
            }
            else
            {
                if ( srcOffset == 0 )
                {
                    record.setCharData( src );
                }
                else
                {
                    byte data[] = new byte[(src.length - srcOffset) * 2];
                    CharBuffer charBuf = ByteBuffer.wrap( data ).asCharBuffer();
                    charBuf.put( src, srcOffset, src.length - srcOffset );
                    record.setData( data );
                }
                nextBlock = Record.NO_NEXT_BLOCK.intValue();
                record.setNextBlock( nextBlock );
            }
            recordList.add( record );
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
                DynamicRecord record = getRecord( blockId, window, false );
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

    private DynamicRecord getRecord( long blockId, PersistenceWindow window, boolean loadData )
    {
        DynamicRecord record = new DynamicRecord( blockId );
        Buffer buffer = window.getOffsettedBuffer( blockId );
        
        // [    ,   x] in use
        // [xxxx,    ] high bits for prev block
        long inUseByte = buffer.get();
        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
        if ( !inUse )
        {
            throw new InvalidRecordException( "Not in use, blockId[" + blockId + "]" );
        }
        long prevBlock = buffer.getUnsignedInt();
        long prevModifier = (inUseByte & 0xF0L) << 28;
        
        int dataSize = getBlockSize() - BLOCK_HEADER_SIZE;
        
        // [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] number of bytes
        // [    ,xxxx][    ,    ][    ,    ][    ,    ] higher bits for next block
        long nrOfBytesInt = buffer.getInt();
        
        int nrOfBytes = (int)(nrOfBytesInt & 0xFFFFFF);
        
        long nextBlock = buffer.getUnsignedInt();
        long nextModifier = (nrOfBytesInt & 0xF000000L) << 8;
        
        long longNextBlock = longFromIntAndMod( nextBlock, nextModifier );
        if ( longNextBlock != Record.NO_NEXT_BLOCK.intValue()
            && nrOfBytes < dataSize || nrOfBytes > dataSize )
        {
            throw new InvalidRecordException( "Next block set[" + nextBlock
                + "] current block illegal size[" + nrOfBytes + "/" + dataSize
                + "]" );
        }
        record.setInUse( true );
        record.setLength( nrOfBytes );
        record.setPrevBlock( longFromIntAndMod( prevBlock, prevModifier ) );
        record.setNextBlock( longNextBlock );
        if ( loadData )
        {
            byte byteArrayElement[] = new byte[nrOfBytes];
            buffer.get( byteArrayElement );
            record.setData( byteArrayElement );
        }
        else
        {
            record.setIsLight( true );
        }
        return record;
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
                DynamicRecord record = getRecord( blockId, window, true );
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
                byte inUse = byteBuffer.get();
                byteBuffer.clear();
                if ( inUse != 0 )
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
        File file = new File( getStorageFileName() + ".id" );
        if ( file.exists() )
        {
            boolean success = file.delete();
            assert success;
        }
        createIdGenerator( getStorageFileName() + ".id" );
        openIdGenerator();
//        nextBlockId(); // reserved first block containing blockSize
        setHighId( 1 );
        FileChannel fileChannel = getFileChannel();
        long highId = 0;
        long defraggedCount = 0;
        try
        {
            long fileSize = fileChannel.size();
            boolean fullRebuild = true;
            if ( getConfig() != null )
            {
                String mode = (String) 
                    getConfig().get( "rebuild_idgenerators_fast" );
                if ( mode != null && mode.toLowerCase().equals( "true" ) )
                {
                    fullRebuild = false;
                    highId = findHighIdBackwards();
                }
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap( new byte[1] );
            LinkedList<Long> freeIdList = new LinkedList<Long>();
            if ( fullRebuild )
            {
                for ( long i = 1; i * getBlockSize() < fileSize; i++ )
                {
                    fileChannel.position( i * getBlockSize() );
                    fileChannel.read( byteBuffer );
                    byteBuffer.flip();
                    byte inUse = byteBuffer.get();
                    byteBuffer.flip();
                    nextBlockId();
                    if ( inUse == Record.NOT_IN_USE.byteValue() )
                    {
                        freeIdList.add( i );
                    }
                    else
                    {
                        highId = i;
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
        if ( getConfig() != null )
        {
            String storeDir = (String) getConfig().get( "store_dir" );
            StringLogger msgLog = StringLogger.getLogger( storeDir );
            msgLog.logMessage( getStorageFileName() + " rebuild id generator, highId=" + getHighId() + 
                    " defragged count=" + defraggedCount, true );
        }
        closeIdGenerator();
        openIdGenerator();
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
}
