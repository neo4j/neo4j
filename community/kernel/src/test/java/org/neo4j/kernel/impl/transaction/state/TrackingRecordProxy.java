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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.transaction.state.RelationshipCreatorTest.Tracker;

public class TrackingRecordProxy<RECORD, ADDITIONAL> implements RecordProxy<Long, RECORD, ADDITIONAL>
{
    private final RecordProxy<Long, RECORD, ADDITIONAL> delegate;
    private final Tracker tracker;
    private final boolean created;
    private boolean changed;

    public TrackingRecordProxy( RecordProxy<Long, RECORD, ADDITIONAL> delegate, boolean created, Tracker tracker )
    {
        this.delegate = delegate;
        this.created = created;
        this.tracker = tracker;
        this.changed = created;
    }

    @Override
    public Long getKey()
    {
        return delegate.getKey();
    }

    @Override
    public RECORD forChangingLinkage()
    {
        trackChange();
        return delegate.forChangingLinkage();
    }

    private void trackChange()
    {
        if ( !created && !changed )
        {
            tracker.changingRelationship( getKey() );
            changed = true;
        }
    }

    @Override
    public RECORD forChangingData()
    {
        trackChange();
        return delegate.forChangingData();
    }

    @Override
    public RECORD forReadingLinkage()
    {
        return delegate.forReadingLinkage();
    }

    @Override
    public RECORD forReadingData()
    {
        return delegate.forReadingData();
    }

    @Override
    public ADDITIONAL getAdditionalData()
    {
        return delegate.getAdditionalData();
    }

    @Override
    public RECORD getBefore()
    {
        return delegate.getBefore();
    }

    @Override
    public boolean isChanged()
    {
        return changed;
    }

    @Override
    public boolean isCreated()
    {
        return created;
    }
}