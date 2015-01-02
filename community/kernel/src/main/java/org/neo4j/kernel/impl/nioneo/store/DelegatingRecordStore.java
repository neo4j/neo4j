/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Collection;

public class DelegatingRecordStore<R extends AbstractBaseRecord> implements RecordStore<R>
{
    private final RecordStore<R> delegate;

    public DelegatingRecordStore( RecordStore<R> delegate )
    {
        this.delegate = delegate;
    }
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public File getStorageFileName()
    {
        return delegate.getStorageFileName();
    }

    @Override
    public WindowPoolStats getWindowPoolStats()
    {
        return delegate.getWindowPoolStats();
    }

    @Override
    public long getHighId()
    {
        return delegate.getHighId();
    }

    @Override
    public long getHighestPossibleIdInUse()
    {
        return delegate.getHighestPossibleIdInUse();
    }

    @Override
    public long nextId()
    {
        return delegate.nextId();
    }

    @Override
    public R getRecord( long id )
    {
        return delegate.getRecord( id );
    }

    public Long getNextRecordReference( R record )
    {
        return delegate.getNextRecordReference( record );
    }

    @Override
    public Collection<R> getRecords( long id )
    {
        return delegate.getRecords( id );
    }

    public void updateRecord( R record )
    {
        delegate.updateRecord( record );
    }

    @Override
    public R forceGetRecord( long id )
    {
        return delegate.forceGetRecord( id );
    }

    public R forceGetRaw( R record )
    {
        return delegate.forceGetRaw( record );
    }

    @Override
    public R forceGetRaw( long id )
    {
        return delegate.forceGetRaw( id );
    }

    public void forceUpdateRecord( R record )
    {
        delegate.forceUpdateRecord( record );
    }

    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, R record ) throws FAILURE
    {
        delegate.accept( processor, record );
    }

    @Override
    public int getRecordSize()
    {
        return delegate.getRecordSize();
    }

    @Override
    public int getRecordHeaderSize()
    {
        return delegate.getRecordHeaderSize();
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
