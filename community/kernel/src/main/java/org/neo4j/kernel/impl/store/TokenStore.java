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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.core.TokenFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.logging.LogProvider;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.kernel.impl.store.PropertyStore.decodeString;

public abstract class TokenStore<RECORD extends TokenRecord, TOKEN extends Token> extends AbstractRecordStore<RECORD>
{
    public static final int NAME_STORE_BLOCK_SIZE = 30;

    private final DynamicStringStore nameStore;
    private final TokenFactory<TOKEN> tokenFactory;

    public TokenStore(
            File file,
            Config configuration,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            DynamicStringStore nameStore,
            TokenFactory<TOKEN> tokenFactory )
    {
        super( file, configuration, idType, idGeneratorFactory, pageCache, logProvider);
        this.nameStore = nameStore;
        this.tokenFactory = tokenFactory;
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
    protected boolean doFastIdGeneratorRebuild()
    {
        return false;
    }

    public List<TOKEN> getTokens( int maxCount )
    {
        LinkedList<TOKEN> records = new LinkedList<>();
        long maxIdInUse = getHighestPossibleIdInUse();
        int found = 0;
        for ( int i = 0; i <= maxIdInUse && found < maxCount; i++ )
        {
            RECORD record;
            try
            {
                record = getRecord( i );
            }
            catch ( InvalidRecordException t )
            {
                continue;
            }
            found++;
            if ( record != null && record.inUse() && record.getNameId() != Record.RESERVED.intValue() )
            {
                records.add( tokenFactory.newToken( getStringFor( record ), i ) );
            }
        }

        return records;
    }

    public TOKEN getToken( int id )
    {
        RECORD record = getRecord( id );
        return tokenFactory.newToken( getStringFor( record ), record.getId() );
    }

    public RECORD getRecord( int id )
    {
        RECORD record = newRecord( id );
        byte inUseByte = Record.NOT_IN_USE.byteValue();

        try ( PageCursor cursor = storeFile.io( pageIdForRecord( id ), PF_SHARED_LOCK ) )
        {
            if ( cursor.next() )
            {
                do
                {
                    inUseByte = getRecord( id, record, cursor );
                } while ( cursor.shouldRetry() );

            }

            if ( inUseByte != Record.IN_USE.byteValue() )
            {
                throw new InvalidRecordException( getClass().getSimpleName() + " Record[" + id + "] not in use" );
            }

            checkInUseByteValidity( id, inUseByte );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }

        record.addNameRecords( nameStore.getLightRecords( record.getNameId() ) );
        return record;
    }

    @Override
    public RECORD getRecord( long id )
    {
        return getRecord( (int) id );
    }

    @Override
    public RECORD forceGetRecord( long id )
    {
        try ( PageCursor cursor = storeFile.io( pageIdForRecord( id ), PF_SHARED_LOCK ) )
        {
            if ( cursor.next() )
            {
                RECORD record = newRecord( (int) id );
                do
                {
                    getRecord( (int) id, record, cursor );
                }
                while ( cursor.shouldRetry() );

                record.setIsLight( true );
                return record;
            }
            else
            {
                return newRecord( (int) id );
            }
        }
        catch ( IOException e )
        {
            return newRecord( (int) id );
        }
    }

    private void checkInUseByteValidity( long id, byte inUseByte )
    {
        if ( inUseByte != Record.IN_USE.byteValue() && inUseByte != Record.NOT_IN_USE.byteValue() )
        {
            throw new InvalidRecordException( getClass().getSimpleName() + " Record[" + id + "] unknown in use flag[" + inUseByte + "]" );
        }
    }

    public Collection<DynamicRecord> allocateNameRecords( byte[] chars )
    {
        Collection<DynamicRecord> records = new ArrayList<>();
        nameStore.allocateRecordsFromBytes( records, chars );
        return records;
    }

    @Override
    public void updateRecord( RECORD record )
    {
        forceUpdateRecord( record );
        if ( !record.isLight() )
        {
            for ( DynamicRecord keyRecord : record.getNameRecords() )
            {
                nameStore.updateRecord( keyRecord );
            }
        }
    }

    @Override
    public void forceUpdateRecord( RECORD record )
    {
        try ( PageCursor cursor = storeFile.io( pageIdForRecord( record.getId() ), PF_EXCLUSIVE_LOCK ) )
        {
            if ( cursor.next() )
            {
                do
                {
                    updateRecord( record, cursor );
                } while ( cursor.shouldRetry() );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    protected abstract RECORD newRecord( int id );

    protected byte getRecord( int id, RECORD record, PageCursor cursor )
    {
        cursor.setOffset( offsetForId( id ) );
        byte inUseByte = cursor.getByte();
        boolean inUse = (inUseByte == Record.IN_USE.byteValue());

        record.setInUse( inUse );
        if ( inUse )
        {
            readRecord( record, cursor );
        }
        return inUseByte;
    }

    protected void readRecord( RECORD record, PageCursor cursor )
    {
        record.setNameId( cursor.getInt() );
    }

    protected void updateRecord( RECORD record, PageCursor cursor )
    {
        int id = record.getId();
        cursor.setOffset( offsetForId( id ) );
        if ( record.inUse() )
        {
            cursor.putByte( Record.IN_USE.byteValue() );
            writeRecord( record, cursor );
        }
        else
        {
            cursor.putByte( Record.NOT_IN_USE.byteValue() );
            freeId( id );
        }
    }

    protected void writeRecord( RECORD record, PageCursor cursor )
    {
        cursor.putInt( record.getNameId() );
    }

    public void ensureHeavy( RECORD record )
    {
        if (!record.isLight())
        {
            return;
        }

        record.setIsLight( false );
        record.addNameRecords( nameStore.getRecords( record.getNameId() ) );
    }

    public String getStringFor( RECORD nameRecord )
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
}
