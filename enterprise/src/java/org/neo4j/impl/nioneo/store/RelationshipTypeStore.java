/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the relationship type store. Uses a dynamic store to store
 * relationship type names.
 */
public class RelationshipTypeStore extends AbstractStore implements Store
{
    // store version, each store ends with this string (byte encoded)
    private static final String VERSION = "RelationshipTypeStore v0.9.5";

    // record header size
    // in_use(byte)+type_blockId(int)
    private static final int RECORD_SIZE = 5;

    private static final int TYPE_STORE_BLOCK_SIZE = 30;

    private DynamicStringStore typeNameStore;

    /**
     * See {@link AbstractStore#AbstractStore(String, Map)}
     */
    public RelationshipTypeStore( String fileName, Map<?,?> config )
    {
        super( fileName, config );
    }

    /**
     * See {@link AbstractStore#AbstractStore(String)}
     */
    public RelationshipTypeStore( String fileName )
    {
        super( fileName );
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
            getStorageFileName() + ".names", getConfig() );
    }

    @Override
    protected void closeStorage()
    {
        typeNameStore.close();
        typeNameStore = null;
    }

    public void flushAll()
    {
        typeNameStore.flushAll();
        super.flushAll();
    }

    public String getTypeAndVersionDescriptor()
    {
        return VERSION;
    }

    public int getRecordSize()
    {
        return RECORD_SIZE;
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
    public static void createStore( String fileName )
    {
        createEmptyStore( fileName, VERSION );
        DynamicStringStore.createStore( fileName + ".names",
            TYPE_STORE_BLOCK_SIZE );
        RelationshipTypeStore store = new RelationshipTypeStore( fileName );
        store.markAsReserved( store.nextId() );
        store.markAsReserved( store.nextId() );
        store.markAsReserved( store.nextId() );
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
        char src[] )
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

    public RelationshipTypeRecord getRecord( int id )
    {
        RelationshipTypeRecord record;
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            record = getRecord( id, window );
        }
        finally
        {
            releaseWindow( window );
        }
        // }
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
            catch ( StoreFailureException e )
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

    public int nextBlockId()
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

    private RelationshipTypeRecord getRecord( int id, PersistenceWindow window )
    {
        Buffer buffer = window.getOffsettedBuffer( id );
        byte inUse = buffer.get();
        if ( inUse == Record.NOT_IN_USE.byteValue() )
        {
            return null;
        }
        if ( inUse != Record.IN_USE.byteValue() )
        {
            throw new StoreFailureException( "Record[" + id + 
                "] unknown in use flag[" + inUse + "]" );
        }
        RelationshipTypeRecord record = new RelationshipTypeRecord( id );
        record.setInUse( true );
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
        IdGenerator.createGenerator( getStorageFileName() + ".id" );
        openIdGenerator();
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
                nextId();
            }
            highId++;
            fileChannel.truncate( highId * recordSize );
        }
        catch ( IOException e )
        {
            throw new StoreFailureException( "Unable to rebuild id generator "
                + getStorageFileName(), e );
        }
        setHighId( highId );
        logger.fine( "[" + getStorageFileName() + "] high id=" + getHighId() );
        closeIdGenerator();
        openIdGenerator();
    }

    public String getStringFor( RelationshipTypeRecord relTypeRecord )
    {
        int recordToFind = relTypeRecord.getTypeBlock();
        Iterator<DynamicRecord> records = 
            relTypeRecord.getTypeRecords().iterator();
        List<char[]> charList = new LinkedList<char[]>();
        int totalSize = 0;
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() && 
            records.hasNext() )
        {
            DynamicRecord record = records.next();
            if ( record.inUse() && record.getId() == recordToFind )
            {
                if ( record.isLight() )
                {
                    typeNameStore.makeHeavy( record );
                }
                if ( !record.isCharData() )
                {
                    ByteBuffer buf = ByteBuffer.wrap( record.getData() );
                    char[] chars = new char[record.getData().length / 2];
                    totalSize += chars.length;
                    buf.asCharBuffer().get( chars );
                    charList.add( chars );
                }
                else
                {
                    charList.add( record.getDataAsChar() );
                }
                recordToFind = record.getNextBlock();
                // TODO: optimize here, high chance next is right one
                records = relTypeRecord.getTypeRecords().iterator();
            }
        }
        StringBuffer buf = new StringBuffer();
        for ( char[] str : charList )
        {
            buf.append( str );
        }
        return buf.toString();
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


    @Override
    protected boolean versionFound( String version )
    {
        if ( !version.startsWith( "RelationshipTypeStore" ) )
        {
            // non clean shutdown, need to do recover with right neo
            return false;
        }
        if ( version.equals( "RelationshipTypeStore v0.9.3" ) )
        {
            rebuildIdGenerator();
            closeIdGenerator();
            return true;
        }
        throw new RuntimeException( "Unknown store version " + version  + 
            " Please make sure you are not running old Neo4j kernel " + 
            " towards a store that has been created by newer version " + 
            " of Neo4j." );
    }
}