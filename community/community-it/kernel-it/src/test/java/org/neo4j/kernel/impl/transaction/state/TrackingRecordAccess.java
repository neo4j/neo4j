/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Collection;

import org.neo4j.internal.recordstorage.RecordAccess;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.transaction.state.RelationshipCreatorTest.Tracker;

public class TrackingRecordAccess<RECORD> implements RecordAccess<RECORD>
{
    private final RecordAccess<RECORD> delegate;
    private final Tracker tracker;

    public TrackingRecordAccess( RecordAccess<RECORD> delegate, Tracker tracker )
    {
        this.delegate = delegate;
        this.tracker = tracker;
    }

    @Override
    public RecordProxy<RECORD> getOrLoad( long key, PageCursorTracer cursorTracer )
    {
        return new TrackingRecordProxy<>( delegate.getOrLoad( key, cursorTracer ), false, tracker );
    }

    @Override
    public RecordProxy<RECORD> create( long key, PageCursorTracer cursorTracer )
    {
        return new TrackingRecordProxy<>( delegate.create( key, cursorTracer ), true, tracker );
    }

    @Override
    public RecordProxy<RECORD> getIfLoaded( long key )
    {
        RecordProxy<RECORD> actual = delegate.getIfLoaded( key );
        return actual == null ? null : new TrackingRecordProxy<>( actual, false, tracker );
    }

    @Override
    public RecordProxy<RECORD> setRecord( long key, RECORD record, PageCursorTracer cursorTracer )
    {
        return delegate.setRecord( key, record, cursorTracer );
    }

    @Override
    public int changeSize()
    {
        return delegate.changeSize();
    }

    @Override
    public Collection<? extends RecordProxy<RECORD>> changes()
    {
        return delegate.changes();
    }
}
