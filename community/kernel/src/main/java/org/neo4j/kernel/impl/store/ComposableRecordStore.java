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

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.LogProvider;

public class ComposableRecordStore<RECORD extends AbstractBaseRecord, HEADER extends StoreHeader>
        extends CommonAbstractStore<RECORD>
{
    protected final RecordFormat<RECORD> recordFormat;
    protected final StoreHeaderFormat<HEADER> storeHeaderFormat;
    protected HEADER storeHeader;
    private int recordSize;

    public ComposableRecordStore( File fileName, Config configuration, IdType idType,
            IdGeneratorFactory idGeneratorFactory, PageCache pageCache, LogProvider logProvider, String typeDescriptor,
            RecordFormat<RECORD> recordFormat, String storeVersion,
            StoreHeaderFormat<HEADER> storeHeaderFormat )
    {
        super( fileName, configuration, idType, idGeneratorFactory, pageCache, logProvider, typeDescriptor,
                storeVersion );
        this.recordFormat = recordFormat;
        this.storeHeaderFormat = storeHeaderFormat;
    }

    @Override
    public RECORD newRecord()
    {
        return recordFormat.newRecord();
    }

    @Override
    public int getRecordSize()
    {
        return recordSize;
    }

    @Override
    public int getRecordDataSize()
    {
        return recordSize - recordFormat.getRecordHeaderSize();
    }

    @Override
    protected void readRecord( PageCursor cursor, RECORD record, RecordLoad mode ) throws IOException
    {
        recordFormat.read( record, cursor, mode, recordSize, storeFile );
    }

    @Override
    protected void writeRecord( PageCursor cursor, RECORD record ) throws IOException
    {
        recordFormat.write( record, cursor, recordSize, storeFile );
    }

    @Override
    public long getNextRecordReference( RECORD record )
    {
        return recordFormat.getNextRecordReference( record );
    }

    @Override
    protected void createHeaderRecord( PageCursor cursor )
    {
        storeHeaderFormat.writeHeader( cursor );
    }

    @Override
    public int getNumberOfReservedLowIds()
    {
        return storeHeaderFormat.numberOfReservedRecords();
    }

    @Override
    protected void readHeaderAndInitializeRecordFormat( PageCursor cursor ) throws IOException
    {
        storeHeader = storeHeaderFormat.readHeader( cursor );
        recordSize = recordFormat.getRecordSize( storeHeader );
    }

    @Override
    protected boolean isInUse( PageCursor cursor )
    {
        return recordFormat.isInUse( cursor );
    }

    @Override
    public int getStoreHeaderInt()
    {
        return ((IntStoreHeader) storeHeader).value();
    }

    @Override
    public <FAILURE extends Exception> void
            accept( org.neo4j.kernel.impl.store.RecordStore.Processor<FAILURE> processor, RECORD record ) throws FAILURE
    {
        throw new UnsupportedOperationException();
    }
}
