/**
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
package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.kernel.impl.nioneo.xa.RelationshipCreatorTest.Tracker;

public class TrackingRecordAccess<RECORD, ADDITIONAL> implements RecordAccess<Long, RECORD, ADDITIONAL>
{
    private final RecordAccess<Long, RECORD, ADDITIONAL> delegate;
    private final Tracker tracker;

    public TrackingRecordAccess( RecordAccess<Long, RECORD, ADDITIONAL> delegate, Tracker tracker )
    {
        this.delegate = delegate;
        this.tracker = tracker;
    }

    @Override
    public RecordProxy<Long, RECORD, ADDITIONAL> getOrLoad( Long key, ADDITIONAL additionalData )
    {
        return new TrackingRecordProxy<>( delegate.getOrLoad( key, additionalData ), false, tracker );
    }

    @Override
    public RecordProxy<Long, RECORD, ADDITIONAL> create( Long key, ADDITIONAL additionalData )
    {
        return new TrackingRecordProxy<>( delegate.create( key, additionalData ), true, tracker );
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
