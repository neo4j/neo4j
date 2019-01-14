/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.impl.transaction.state.RelationshipCreatorTest.Tracker;

public class TrackingRecordAccess<RECORD, ADDITIONAL> implements RecordAccess<RECORD, ADDITIONAL>
{
    private final RecordAccess<RECORD, ADDITIONAL> delegate;
    private final Tracker tracker;

    public TrackingRecordAccess( RecordAccess<RECORD, ADDITIONAL> delegate, Tracker tracker )
    {
        this.delegate = delegate;
        this.tracker = tracker;
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> getOrLoad( long key, ADDITIONAL additionalData )
    {
        return new TrackingRecordProxy<>( delegate.getOrLoad( key, additionalData ), false, tracker );
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> create( long key, ADDITIONAL additionalData )
    {
        return new TrackingRecordProxy<>( delegate.create( key, additionalData ), true, tracker );
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public RecordProxy<RECORD,ADDITIONAL> getIfLoaded( long key )
    {
        RecordProxy<RECORD,ADDITIONAL> actual = delegate.getIfLoaded( key );
        return actual == null ? null : new TrackingRecordProxy<>( actual, false, tracker );
    }

    @Override
    public void setTo( long key, RECORD newRecord, ADDITIONAL additionalData )
    {
        delegate.setTo( key, newRecord, additionalData );
    }

    @Override
    public RecordProxy<RECORD,ADDITIONAL> setRecord( long key, RECORD record, ADDITIONAL additionalData )
    {
        return delegate.setRecord( key, record, additionalData );
    }

    @Override
    public int changeSize()
    {
        return delegate.changeSize();
    }

    @Override
    public Iterable<RecordProxy<RECORD,ADDITIONAL>> changes()
    {
        return new IterableWrapper<RecordProxy<RECORD,ADDITIONAL>,RecordProxy<RECORD,ADDITIONAL>>(
                delegate.changes() )
        {
            @Override
            protected RecordProxy<RECORD,ADDITIONAL> underlyingObjectToObject(
                    RecordProxy<RECORD,ADDITIONAL> actual )
            {
                return new TrackingRecordProxy<>( actual, false, tracker );
            }
        };
    }
}
