/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * Implementation of the node store.
 */
public class PropertyIndexStore extends AbstractStore implements Store
{
    // store version, should end with this string (byte encoded)
    private static final String VERSION = "PropertyIndex v0.9.3";
    private static final int KEY_STORE_BLOCK_SIZE = 30;

    // in_use(byte)+prop_count(int)+key_block_id(int)
    private static final int RECORD_SIZE = 9;

    private DynamicStringStore keyPropertyStore;

    public PropertyIndexStore( String fileName, Map<?,?> config )
    {
        super( fileName, config );
    }

    public PropertyIndexStore( String fileName )
    {
        super( fileName );
    }

    protected void initStorage()
    {
        keyPropertyStore = new DynamicStringStore( getStorageFileName()
            + ".keys", getConfig() );
    }

    public String getTypeAndVersionDescriptor()
    {
        return VERSION;
    }

    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    @Override
    protected void setRecovered()
    {
        super.setRecovered();
        keyPropertyStore.setRecovered();
    }

    @Override
    protected void unsetRecovered()
    {
        super.unsetRecovered();
        keyPropertyStore.unsetRecovered();
    }

    @Override
    public void makeStoreOk()
    {
        keyPropertyStore.makeStoreOk();
        super.makeStoreOk();
    }

    @Override
    public void rebuildIdGenerators()
    {
        keyPropertyStore.rebuildIdGenerators();
        super.rebuildIdGenerators();
    }
    
    public void freeBlockId( int id )
    {
        keyPropertyStore.freeBlockId( id );
    }

    @Override
    protected void closeStorage()
    {
        keyPropertyStore.close();
        keyPropertyStore = null;
    }

    @Override
    public void flushAll()
    {
        keyPropertyStore.flushAll();
        super.flushAll();
    }

    public static void createStore( String fileName )
    {
        createEmptyStore( fileName, VERSION );
        DynamicStringStore.createStore( fileName + ".keys",
            KEY_STORE_BLOCK_SIZE );
    }

    public PropertyIndexData[] getPropertyIndexes( int count )
    {
        LinkedList<PropertyIndexData> indexList = 
            new LinkedList<PropertyIndexData>();
        int maxIdInUse = getHighestPossibleIdInUse();
        int found = 0;
        for ( int i = 0; i <= maxIdInUse && found < count; i++ )
        {
            PropertyIndexRecord record;
            try
            {
                record = getRecord( i );
            }
            catch ( StoreFailureException t )
            {
                continue;
            }
            found++;
            indexList.add( new PropertyIndexData( record.getId(), 
                getStringFor( record ) ) );
        }
        return indexList.toArray( new PropertyIndexData[indexList.size()] );
    }

    public PropertyIndexRecord getRecord( int id )
    {
        PropertyIndexRecord record;
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            record = getRecord( id, window.getBuffer() );
        }
        finally
        {
            releaseWindow( window );
        }
        Collection<DynamicRecord> keyRecords = 
            keyPropertyStore.getLightRecords( record.getKeyBlockId() );
        for ( DynamicRecord keyRecord : keyRecords )
        {
            record.addKeyRecord( keyRecord );
        }
        return record;
    }

    public PropertyIndexRecord getLightRecord( int id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            PropertyIndexRecord record = getRecord( id, window.getBuffer() );
            record.setIsLight( true );
            return record;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public void updateRecord( PropertyIndexRecord record, boolean recovered )
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

    public void updateRecord( PropertyIndexRecord record )
    {
        PersistenceWindow window = acquireWindow( record.getId(),
            OperationType.WRITE );
        try
        {
            updateRecord( record, window.getBuffer() );
        }
        finally
        {
            releaseWindow( window );
        }
        if ( !record.isLight() )
        {
            for ( DynamicRecord keyRecord : record.getKeyRecords() )
            {
                keyPropertyStore.updateRecord( keyRecord );
            }
        }
    }

    public Collection<DynamicRecord> allocateKeyRecords( int keyBlockId,
        char[] chars )
    {
        return keyPropertyStore.allocateRecords( keyBlockId, chars );
    }

    public int nextKeyBlockId()
    {
        return keyPropertyStore.nextBlockId();
    }

    private PropertyIndexRecord getRecord( int id, Buffer buffer )
    {
        int offset = (int) (id - buffer.position()) * getRecordSize();
        buffer.setOffset( offset );
        boolean inUse = (buffer.get() == Record.IN_USE.byteValue());
        if ( !inUse )
        {
            throw new StoreFailureException( "Record[" + id + "] not in use" );
        }
        PropertyIndexRecord record = new PropertyIndexRecord( id );
        record.setInUse( inUse );
        record.setPropertyCount( buffer.getInt() );
        record.setKeyBlockId( buffer.getInt() );
        return record;
    }

    private void updateRecord( PropertyIndexRecord record, Buffer buffer )
    {
        int id = record.getId();
        int offset = (int) (id - buffer.position()) * getRecordSize();
        buffer.setOffset( offset );
        if ( record.inUse() )
        {
            buffer.put( Record.IN_USE.byteValue() ).putInt(
                record.getPropertyCount() ).putInt( record.getKeyBlockId() );
        }
        else
        {
            buffer.put( Record.NOT_IN_USE.byteValue() );
            if ( !isInRecoveryMode() )
            {
                freeId( id );
            }
        }
    }

    public void makeHeavy( PropertyIndexRecord record )
    {
        record.setIsLight( false );
        Collection<DynamicRecord> keyRecords = keyPropertyStore.getRecords(
            record.getKeyBlockId() );
        for ( DynamicRecord keyRecord : keyRecords )
        {
            record.addKeyRecord( keyRecord );
        }
    }

    public String getStringFor( PropertyIndexRecord propRecord )
    {
        int recordToFind = propRecord.getKeyBlockId();
        Iterator<DynamicRecord> records = propRecord.getKeyRecords().iterator();
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
                    keyPropertyStore.makeHeavy( record );
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
                records = propRecord.getKeyRecords().iterator();
            }
        }
        StringBuffer buf = new StringBuffer();
        for ( char[] str : charList )
        {
            buf.append( str );
        }
        return buf.toString();
    }

    public String toString()
    {
        return "PropertyIndexStore";
    }
}