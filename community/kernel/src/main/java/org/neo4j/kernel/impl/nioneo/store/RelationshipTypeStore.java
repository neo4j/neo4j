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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Implementation of the relationship type store. Uses a dynamic store to store
 * relationship type names.
 */
public class RelationshipTypeStore extends AbstractStore implements Store, RecordStore<RelationshipTypeRecord>
{
    public static final String TYPE_DESCRIPTOR = "RelationshipTypeStore";

    // record header size
    // in_use(byte)+type_blockId(int)
    private static final int RECORD_SIZE = 5;

    private static final int TYPE_STORE_BLOCK_SIZE = 30;

    private DynamicStringStore typeNameStore;

    public RelationshipTypeStore( String fileName, Map<?,?> config, IdType idType )
    {
        super( fileName, config, idType );
    }

    @Override
    public void accept( RecordStore.Processor processor, RelationshipTypeRecord record )
    {
        processor.processRelationshipType( this, record );
    }

    DynamicStringStore getNameStore()
    {
        return typeNameStore;
    }

    @Override
    protected void setRecovered()
    {
        super.setRecovered();
        typeNameStore.setRecovered();
    }

    @Override
    protected void unsetRecovered()
    {
        super.unsetRecovered();
        typeNameStore.unsetRecovered();
    }

    @Override
    protected void initStorage()
    {
        typeNameStore = new DynamicStringStore(
            getStorageFileName() + ".names", getConfig(), IdType.RELATIONSHIP_TYPE_BLOCK );
    }

    @Override
    protected void closeStorage()
    {
        if ( typeNameStore != null )
        {
            typeNameStore.close();
            typeNameStore = null;
        }
    }

    @Override
    public void flushAll()
    {
        typeNameStore.flushAll();
        super.flushAll();
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    @Override
    public int getRecordHeaderSize()
    {
        return getRecordSize();
    }

    /**
     * Creates a new relationship type store contained in <CODE>fileName</CODE>
     * If filename is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     *
     * @param fileName
     *            File name of the new relationship type store
     * @throws IOException
     *             If unable to create store or name null
     */
    public static void createStore( String fileName, Map<?, ?> config )
    {
        IdGeneratorFactory idGeneratorFactory = (IdGeneratorFactory) config.get(
                IdGeneratorFactory.class );
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ), idGeneratorFactory );
        DynamicStringStore.createStore( fileName + ".names",
            TYPE_STORE_BLOCK_SIZE, idGeneratorFactory, IdType.RELATIONSHIP_TYPE_BLOCK );
        RelationshipTypeStore store = new RelationshipTypeStore(
                fileName, config, IdType.RELATIONSHIP_TYPE );
        store.close();
    }

    void markAsReserved( int id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.WRITE );
        try
        {
            markAsReserved( id, window );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public Collection<DynamicRecord> allocateTypeNameRecords( int startBlock,
        byte src[] )
    {
        return typeNameStore.allocateRecords( startBlock, src );
    }

    public void updateRecord( RelationshipTypeRecord record, boolean recovered )
    {
        assert recovered;
        setRecovered();
        try
        {
            updateRecord( record );
            registerIdFromUpdateRecord( record.getId() );
        }
        finally
        {
            unsetRecovered();
        }
    }

    public void updateRecord( RelationshipTypeRecord record )
    {
        PersistenceWindow window = acquireWindow( record.getId(),
            OperationType.WRITE );
        try
        {
            updateRecord( record, window );
        }
        finally
        {
            releaseWindow( window );
        }
        for ( DynamicRecord typeRecord : record.getTypeRecords() )
        {
            typeNameStore.updateRecord( typeRecord );
        }
    }

    @Override
    public void forceUpdateRecord( RelationshipTypeRecord record )
    {
        PersistenceWindow window = acquireWindow( record.getId(),
                OperationType.WRITE );
        try
        {
            updateRecord( record, window );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public RelationshipTypeRecord getRecord( int id )
    {
        RelationshipTypeRecord record;
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            record = getRecord( id, window, false );
        }
        finally
        {
            releaseWindow( window );
        }
        if ( record != null )
        {
            Collection<DynamicRecord> nameRecords = typeNameStore.getRecords(
                record.getTypeBlock() );
            for ( DynamicRecord nameRecord : nameRecords )
            {
                record.addTypeRecord( nameRecord );
            }
        }
        return record;
    }

    @Override
    public RelationshipTypeRecord getRecord( long id )
    {
        return getRecord( (int) id );
    }

    @Override
    public RelationshipTypeRecord forceGetRecord( long id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            return new RelationshipTypeRecord( (int)id );
        }
        
        try
        {
            return getRecord( (int) id, window, true );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public RelationshipTypeData getRelationshipType( int id, boolean recovered )
    {
        assert recovered;
        try
        {
            setRecovered();
            RelationshipTypeRecord record = getRecord( id );
            String name = getStringFor( record );
            return new RelationshipTypeData( id, name );
        }
        finally
        {
            unsetRecovered();
        }
    }

    public RelationshipTypeData getRelationshipType( int id )
    {
        RelationshipTypeRecord record = getRecord( id );
        String name = getStringFor( record );
        return new RelationshipTypeData( id, name );
    }

    public RelationshipTypeData[] getRelationshipTypes()
    {
        LinkedList<RelationshipTypeData> typeDataList =
            new LinkedList<RelationshipTypeData>();
        for ( int i = 0;; i++ )
        {
            RelationshipTypeRecord record;
            try
            {
                record = getRecord( i );
            }
            catch ( InvalidRecordException e )
            {
                break;
            }
            if ( record != null &&
                record.getTypeBlock() != Record.RESERVED.intValue() )
            {
                String name = getStringFor( record );
                typeDataList.add( new RelationshipTypeData( i, name ) );
            }
        }
        return typeDataList.toArray(
            new RelationshipTypeData[typeDataList.size()] );
    }

    public long nextBlockId()
    {
        return typeNameStore.nextBlockId();
    }

    public void freeBlockId( int id )
    {
        typeNameStore.freeBlockId( id );
    }

    private void markAsReserved( int id, PersistenceWindow window )
    {
        Buffer buffer = window.getOffsettedBuffer( id );
        buffer.put( Record.IN_USE.byteValue() ).putInt(
            Record.RESERVED.intValue() );
    }

    private RelationshipTypeRecord getRecord( int id, PersistenceWindow window, boolean force )
    {
        Buffer buffer = window.getOffsettedBuffer( id );
        byte inUse = buffer.get();
        if ( !force )
        {
            if ( inUse == Record.NOT_IN_USE.byteValue() )
            {
                return null;
            }
            if ( inUse != Record.IN_USE.byteValue() )
            {
                throw new InvalidRecordException( "Record[" + id + "] unknown in use flag[" + inUse + "]" );
            }
        }
        RelationshipTypeRecord record = new RelationshipTypeRecord( id );
        record.setInUse( inUse == Record.IN_USE.byteValue() );
        record.setTypeBlock( buffer.getInt() );
        return record;
    }

    private void updateRecord( RelationshipTypeRecord record,
        PersistenceWindow window )
    {
        int id = record.getId();
        Buffer buffer = window.getOffsettedBuffer( id );
        if ( record.inUse() )
        {
            buffer.put( Record.IN_USE.byteValue() ).putInt(
                record.getTypeBlock() );
        }
        else
        {
            buffer.put( Record.NOT_IN_USE.byteValue() ).putInt( 0 );
        }
    }

    @Override
    protected void rebuildIdGenerator()
    {
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
        openIdGenerator( false );
        FileChannel fileChannel = getFileChannel();
        long highId = -1;
        int recordSize = getRecordSize();
        try
        {
            long fileSize = fileChannel.size();
            ByteBuffer byteBuffer = ByteBuffer.wrap( new byte[recordSize] );
            for ( int i = 0; i * recordSize < fileSize; i++ )
            {
                fileChannel.read( byteBuffer, i * recordSize );
                byteBuffer.flip();
                byte inUse = byteBuffer.get();
                byteBuffer.flip();
                if ( inUse != Record.IN_USE.byteValue() )
                {
                    // hole found, marking as reserved
                    byteBuffer.clear();
                    byteBuffer.put( Record.IN_USE.byteValue() ).putInt(
                        Record.RESERVED.intValue() );
                    byteBuffer.flip();
                    fileChannel.write( byteBuffer, i * recordSize );
                    byteBuffer.clear();
                }
                else
                {
                    highId = i;
                }
                // nextId();
            }
            highId++;
            fileChannel.truncate( highId * recordSize );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                "Unable to rebuild id generator " + getStorageFileName(), e );
        }
        setHighId( highId );
        logger.fine( "[" + getStorageFileName() + "] high id=" + getHighId() );
        closeIdGenerator();
        openIdGenerator( false );
    }

    public String getStringFor( RelationshipTypeRecord relTypeRecord )
    {
        long recordToFind = relTypeRecord.getTypeBlock();
        Iterator<DynamicRecord> records = relTypeRecord.getTypeRecords().iterator();
        Collection<DynamicRecord> relevantRecords = new ArrayList<DynamicRecord>();
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() && records.hasNext() )
        {
            DynamicRecord record = records.next();
            if ( record.inUse() && record.getId() == recordToFind )
            {
                recordToFind = record.getNextBlock();
                // TODO: optimize here, high chance next is right one
                relevantRecords.add( record );
                records = relTypeRecord.getTypeRecords().iterator();
            }
        }
        return (String) PropertyStore.getStringFor( PropertyStore.readFullByteArray(
                relTypeRecord.getTypeBlock(), relevantRecords, typeNameStore ) );
    }

    @Override
    public void makeStoreOk()
    {
        typeNameStore.makeStoreOk();
        super.makeStoreOk();
    }

    @Override
    public void rebuildIdGenerators()
    {
        typeNameStore.rebuildIdGenerators();
        super.rebuildIdGenerators();
    }

    public void updateIdGenerators()
    {
        typeNameStore.updateHighId();
        this.updateHighId();
    }

    @Override
    public List<WindowPoolStats> getAllWindowPoolStats()
    {
        List<WindowPoolStats> list = new ArrayList<WindowPoolStats>();
        list.add( typeNameStore.getWindowPoolStats() );
        list.add( getWindowPoolStats() );
        return list;
    }

    @Override
    public void logIdUsage( StringLogger logger )
    {
        NeoStore.logIdUsage( logger, this );
    }
}