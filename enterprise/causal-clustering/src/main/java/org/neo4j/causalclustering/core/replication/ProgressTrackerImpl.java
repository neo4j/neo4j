/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.replication;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.Result;

public class ProgressTrackerImpl implements ProgressTracker
{
    private final Map<LocalOperationId,Progress> tracker = new ConcurrentHashMap<>();
    private final GlobalSession myGlobalSession;

    public ProgressTrackerImpl( GlobalSession myGlobalSession )
    {
        this.myGlobalSession = myGlobalSession;
    }

    @Override
    public Progress start( DistributedOperation operation )
    {
        assert operation.globalSession().equals( myGlobalSession );

        Progress progress = new Progress();
        tracker.put( operation.operationId(), progress );
        return progress;
    }

    @Override
    public void trackReplication( DistributedOperation operation )
    {
        if( !operation.globalSession().equals( myGlobalSession ) )
        {
            return;
        }

        Progress progress = tracker.get( operation.operationId() );
        if( progress != null )
        {
            progress.setReplicated();
        }
    }

    @Override
    public void trackResult( DistributedOperation operation, Result result )
    {
        if( !operation.globalSession().equals( myGlobalSession ) )
        {
            return;
        }

        Progress progress = tracker.remove( operation.operationId() );

        if ( progress != null )
        {
            result.apply( progress.futureResult() );
        }
    }

    @Override
    public void abort( DistributedOperation operation )
    {
        tracker.remove( operation.operationId() );
    }

    @Override
    public void triggerReplicationEvent()
    {
        tracker.forEach( ( ignored, progress ) -> progress.triggerReplicationEvent() );
    }

    @Override
    public int inProgressCount()
    {
        return tracker.size();
    }
}
