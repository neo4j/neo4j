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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Not thread safe, intended for single threaded use.
 */
public class DiffRecordStore<R extends AbstractBaseRecord> implements RecordStore<R>, Iterable<Long>
{
    private final RecordStore<R> actual;
    private final Map<Long, R> diff;
    private long highId = -1;

    public DiffRecordStore( RecordStore<R> actual )
    {
        this.actual = actual;
        this.diff = new HashMap<Long, R>();
    }

    public void markDirty( long id )
    {
        if ( !diff.containsKey( id ) ) diff.put( id, null );
    }

    @Override
    public long getHighId()
    {
        return Math.max( highId, actual.getHighId() );
    }

    @Override
    public R getRecord( long id )
    {
        return getRecord( id, false );
    }

    @Override
    public R forceGetRecord( long id )
    {
        return getRecord( id, true );
    }

    private R getRecord( long id, boolean force )
    {
        R record = diff.get( id );
        if ( record == null ) return actual.getRecord( id );
        if ( !force && !record.inUse() ) throw new InvalidRecordException( "Record[" + id + "] not in use" );
        return record;
    }

    @Override
    public void updateRecord( R record )
    {
        if ( record.getLongId() > highId ) highId = record.getLongId();
        diff.put( record.getLongId(), record );
    }

    @Override
    public void forceUpdateRecord( R record )
    {
        updateRecord( record );
    }

    @Override
    public void accept( RecordStore.Processor processor, R record )
    {
        actual.accept( processor, record );
    }

    @Override
    public Iterator<Long> iterator()
    {
        return diff.keySet().iterator();
    }
}
