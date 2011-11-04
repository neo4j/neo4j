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
 * Implementation of the property store.
 */
public class PropertyIndexStore extends AbstractStore implements Store, RecordStore<PropertyIndexRecord>
{
    public static final String TYPE_DESCRIPTOR = "PropertyIndexStore";

    private static final int KEY_STORE_BLOCK_SIZE = 30;

    // in_use(byte)+prop_count(int)+key_block_id(int)
    private static final int RECORD_SIZE = 9;

    private DynamicStringStore keyPropertyStore;

    public PropertyIndexStore( String fileName, Map<?,?> config )
    {
        super( fileName, config, IdType.PROPERTY_INDEX );
    }

    @Override
    public void accept( RecordStore.Processor processor, PropertyIndexRecord record )
    {
        processor.processPropertyIndex( this, record );
    }

    DynamicStringStore getKeyStore()
    {
        return keyPropertyStore;
    }

    @Override
    protected void initStorage()
    {
        keyPropertyStore = new DynamicStringStore( getStorageFileName()
            + ".keys", getConfig(), IdType.PROPERTY_INDEX_BLOCK );
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

    public void updateIdGenerators()
    {
        keyPropertyStore.updateHighId();
        this.updateHighId();
    }

    public void freeBlockId( int id )
    {
        keyPropertyStore.freeBlockId( id );
    }

    @Override
    protected void closeStorage()
    {
        if ( keyPropertyStore != null )
        {
            keyPropertyStore.close();
            keyPropertyStore = null;
        }
    }

    @Override
    public void flushAll()
    {
        keyPropertyStore.flushAll();
        super.flushAll();
    }

    public static void createStore( String fileName, IdGeneratorFactory idGeneratorFactory )
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ), idGeneratorFactory );
        DynamicStringStore.createStore( fileName + ".keys",
                KEY_STORE_BLOCK_SIZE, idGeneratorFactory, IdType.PROPERTY_INDEX_BLOCK );
    }

    public PropertyIndexData[] getPropertyIndexes( int count )
    {
        LinkedList<PropertyIndexData> indexList =
            new LinkedList<PropertyIndexData>();
        long maxIdInUse = getHighestPossibleIdInUse();
        int found = 0;
        for ( int i = 0; i <= maxIdInUse && found < count; i++ )
        {
            PropertyIndexRecord record;
            try
            {
                record = getRecord( i );
            }
            catch ( InvalidRecordException t )
            {
                continue;
            }
            found++;
            indexList.add( new PropertyIndexData( record.getId(),
                getStringFor( record ) ) );
        }
        return indexList.toArray( new PropertyIndexData[indexList.size()] );
    }

    public PropertyIndexData getPropertyIndex( int id )
    {
        PropertyIndexRecord record = getRecord( id );
        return new PropertyIndexData( record.getId(),
            getStringFor( record ) );
    }

    public PropertyIndexData getPropertyIndex( int id, boolean recovered )
    {
        assert recovered;
        try
        {
            setRecovered();
            PropertyIndexRecord record = getRecord( id );
            return new PropertyIndexData( record.getId(),
                getStringFor( record ) );
        }
        finally
        {
            unsetRecovered();
        }
    }

    public PropertyIndexRecord getRecord( int id )
    {
        PropertyIndexRecord record;
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            record = getRecord( id, window, false );
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

    @Override
    public PropertyIndexRecord getRecord( long id )
    {
        return getRecord( (int) id );
    }

    @Override
    public PropertyIndexRecord forceGetRecord( long id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            return new PropertyIndexRecord( (int)id );
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

    public Collection<DynamicRecord> allocateKeyRecords( int keyBlockId, byte[] chars )
    {
        return keyPropertyStore.allocateRecords( keyBlockId, chars );
    }

    public PropertyIndexRecord getLightRecord( int id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            PropertyIndexRecord record = getRecord( id, window, false );
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
            registerIdFromUpdateRecord( record.getId() );
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
            updateRecord( record, window );
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

    @Override
    public void forceUpdateRecord( PropertyIndexRecord record )
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

    public int nextKeyBlockId()
    {
        return (int) keyPropertyStore.nextBlockId();
    }

    private PropertyIndexRecord getRecord( int id, PersistenceWindow window, boolean force )
    {
        Buffer buffer = window.getOffsettedBuffer( id );
        boolean inUse = (buffer.get() == Record.IN_USE.byteValue());
        if ( !inUse && !force )
        {
            throw new InvalidRecordException( "Record[" + id + "] not in use" );
        }
        PropertyIndexRecord record = new PropertyIndexRecord( id );
        record.setInUse( inUse );
        record.setPropertyCount( buffer.getInt() );
        record.setKeyBlockId( buffer.getInt() );
        return record;
    }

    private void updateRecord( PropertyIndexRecord record,
        PersistenceWindow window )
    {
        int id = record.getId();
        Buffer buffer = window.getOffsettedBuffer( id );
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
        Collection<DynamicRecord> relevantRecords = new ArrayList<DynamicRecord>();
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() &&  records.hasNext() )
        {
            DynamicRecord record = records.next();
            if ( record.inUse() && record.getId() == recordToFind )
            {
                recordToFind = (int) record.getNextBlock();
//                // TODO: optimize here, high chance next is right one
                relevantRecords.add( record );
                records = propRecord.getKeyRecords().iterator();
            }
        }
        return (String) PropertyStore.getStringFor( PropertyStore.readFullByteArray(
                propRecord.getKeyBlockId(), relevantRecords, keyPropertyStore ) );
    }

    @Override
    public List<WindowPoolStats> getAllWindowPoolStats()
    {
        List<WindowPoolStats> list = new ArrayList<WindowPoolStats>();
        list.add( keyPropertyStore.getWindowPoolStats() );
        list.add( getWindowPoolStats() );
        return list;
    }

    @Override
    public void logIdUsage( StringLogger logger )
    {
        NeoStore.logIdUsage( logger, this );
    }
}