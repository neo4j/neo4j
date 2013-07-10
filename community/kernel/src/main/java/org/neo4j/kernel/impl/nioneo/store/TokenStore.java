/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.kernel.impl.nioneo.store.PropertyStore.decodeString;

public abstract class TokenStore<T extends TokenRecord> extends AbstractRecordStore<T> implements Store
{
    public static abstract class Configuration
        extends AbstractStore.Configuration
    {
        
    }
    
    private DynamicStringStore nameStore;
    public static final int NAME_STORE_BLOCK_SIZE = 30;

    public TokenStore( File fileName, Config configuration, IdType idType,
                       IdGeneratorFactory idGeneratorFactory, WindowPoolFactory windowPoolFactory,
                       FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger,
                       DynamicStringStore nameStore )
    {
        super( fileName, configuration, idType, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger );
        this.nameStore = nameStore;
    }

    public DynamicStringStore getNameStore()
    {
        return nameStore;
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
        nameStore.setRecovered();
    }

    @Override
    protected void unsetRecovered()
    {
        super.unsetRecovered();
        nameStore.unsetRecovered();
    }

    @Override
    public void makeStoreOk()
    {
        nameStore.makeStoreOk();
        super.makeStoreOk();
    }

    @Override
    public void rebuildIdGenerators()
    {
        nameStore.rebuildIdGenerators();
        super.rebuildIdGenerators();
    }

    public void updateIdGenerators()
    {
        nameStore.updateHighId();
        this.updateHighId();
    }

    public void freeId( int id )
    {
        nameStore.freeId( id );
    }

    @Override
    protected void closeStorage()
    {
        if ( nameStore != null )
        {
            nameStore.close();
            nameStore = null;
        }
    }

    @Override
    public void flushAll()
    {
        nameStore.flushAll();
        super.flushAll();
    }

    public Token[] getTokens( int maxCount )
    {
        LinkedList<Token> recordList = new LinkedList<>();
        long maxIdInUse = getHighestPossibleIdInUse();
        int found = 0;
        for ( int i = 0; i <= maxIdInUse && found < maxCount; i++ )
        {
            T record;
            try
            {
                record = getRecord( i );
            }
            catch ( InvalidRecordException t )
            {
                continue;
            }
            found++;
            if ( record != null && record.getNameId() != Record.RESERVED.intValue() )
            {
                String name = getStringFor( record );
                recordList.add( new Token( name, i ) );
            }
        }
        return recordList.toArray( new Token[recordList.size()] );
    }

    public Token getToken( int id )
    {
        T record = getRecord( id );
        return new Token( getStringFor( record ), record.getId() );
    }

    public Token getToken( int id, boolean recovered )
    {
        assert recovered;
        try
        {
            setRecovered();
            T record = getRecord( id );
            return new Token( getStringFor( record ), record.getId() );
        }
        finally
        {
            unsetRecovered();
        }
    }

    public T getRecord( int id )
    {
        T record;
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            record = getRecord( id, window, false );
        }
        finally
        {
            releaseWindow( window );
        }
        record.addNameRecords( nameStore.getLightRecords( record.getNameId() ) );
        return record;
    }

    @Override
    public T getRecord( long id )
    {
        return getRecord( (int) id );
    }

    @Override
    public T forceGetRecord( long id )
    {
        PersistenceWindow window;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            return newRecord( (int) id );
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

    @Override
    public T forceGetRaw( T record )
    {
        return record;
    }

    @Override
    public T forceGetRaw( long id )
    {
        return forceGetRecord( id );
    }

    public Collection<DynamicRecord> allocateNameRecords( byte[] chars )
    {
        return nameStore.allocateRecordsFromBytes( chars );
    }

    public T getLightRecord( int id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            T record = getRecord( id, window, false );
            record.setIsLight( true );
            return record;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    @Override
    public void updateRecord( T record )
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
            for ( DynamicRecord keyRecord : record.getNameRecords() )
            {
                nameStore.updateRecord( keyRecord );
            }
        }
    }

    @Override
    public void forceUpdateRecord( T record )
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

    public int nextNameId()
    {
        return (int) nameStore.nextId();
    }

    protected abstract T newRecord( int id );

    protected T getRecord( int id, PersistenceWindow window, boolean force )
    {
        Buffer buffer = window.getOffsettedBuffer( id );
        byte inUseByte = buffer.get();
        boolean inUse = (inUseByte == Record.IN_USE.byteValue());
        if ( !inUse && !force )
        {
            throw new InvalidRecordException( getClass().getSimpleName() + " Record[" + id + "] not in use" );
        }
        if ( inUseByte != Record.IN_USE.byteValue() && inUseByte != Record.NOT_IN_USE.byteValue() )
        {
            throw new InvalidRecordException( getClass().getSimpleName() + " Record[" + id + "] unknown in use flag[" + inUse + "]" );
        }

        T record = newRecord( id );
        record.setInUse( inUse );
        readRecord( record, buffer );
        return record;
    }

    protected void readRecord( T record, Buffer buffer )
    {
        record.setNameId( buffer.getInt() );
    }

    protected void updateRecord( T record, PersistenceWindow window )
    {
        int id = record.getId();
        registerIdFromUpdateRecord( id );
        Buffer buffer = window.getOffsettedBuffer( id );
        if ( record.inUse() )
        {
            buffer.put( Record.IN_USE.byteValue() );
            writeRecord( record, buffer );
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

    protected void writeRecord( T record, Buffer buffer )
    {
        buffer.putInt( record.getNameId() );
    }

    public void ensureHeavy( T record )
    {
        if (!record.isLight())
            return;

        record.setIsLight( false );
        record.addNameRecords( nameStore.getRecords( record.getNameId() ) );
    }

    public String getStringFor( T nameRecord )
    {
        int recordToFind = nameRecord.getNameId();
        Iterator<DynamicRecord> records = nameRecord.getNameRecords().iterator();
        Collection<DynamicRecord> relevantRecords = new ArrayList<>();
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() &&  records.hasNext() )
        {
            DynamicRecord record = records.next();
            if ( record.inUse() && record.getId() == recordToFind )
            {
                recordToFind = (int) record.getNextBlock();
//                // TODO: optimize here, high chance next is right one
                relevantRecords.add( record );
                records = nameRecord.getNameRecords().iterator();
            }
        }
        return decodeString( nameStore.readFullByteArray( relevantRecords, PropertyType.STRING ).other() );
    }

    @Override
    public List<WindowPoolStats> getAllWindowPoolStats()
    {
        List<WindowPoolStats> list = new ArrayList<WindowPoolStats>();
        list.add( nameStore.getWindowPoolStats() );
        list.add( getWindowPoolStats() );
        return list;
    }

    @Override
    public void logAllWindowPoolStats( StringLogger.LineLogger logger )
    {
        super.logAllWindowPoolStats( logger );
        logger.logLine( nameStore.getWindowPoolStats().toString() );
    }
}
