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
package org.neo4j.cypher.result;

import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.QuerySubscription;

/**
 * A query subscription that streams from a materialized result.
 */
public abstract class EagerQuerySubscription implements QuerySubscription
{
    private long requestedRecords;
    private int servedRecords;
    protected final QuerySubscriber subscriber;
    protected boolean cancelled;
    protected Throwable error;

    protected EagerQuerySubscription( QuerySubscriber subscriber )
    {
        this.subscriber = subscriber;
    }

    /**
     * Stream one record to the subscriber.
     *
     * @param servedRecords the number of previously served records
     * @throws Exception if the subscriber throws an Exception
     */
    protected abstract void streamRecordToSubscriber( int servedRecords ) throws Exception;

    /**
     * @return the statistics of the query execution.
     */
    protected abstract QueryStatistics queryStatistics();

    /**
     * @return the size of the materialized result
     */
    protected abstract int resultSize();

    /**
     * Materialize the result if it has not been materialized yet.
     */
    protected abstract void materializeIfNecessary() throws Exception;

    @Override
    public void request( long numberOfRecords ) throws Exception
    {
        requestedRecords = checkForOverflow( requestedRecords + numberOfRecords );
        materializeIfNecessary();
        streamToSubscriber();
    }

    @Override
    public void cancel()
    {
        cancelled = true;
    }

    @Override
    public boolean await()
    {
        boolean hasMore = servedRecords < resultSize();
        if ( !hasMore )
        {
            if ( error != null )
            {
                subscriber.onError( error );
            }
            else
            {
                subscriber.onResultCompleted( queryStatistics() );
            }
        }
        return hasMore && !cancelled;
    }

    private void streamToSubscriber()
    {
        try
        {
            for ( ; servedRecords < requestedRecords && servedRecords < resultSize(); servedRecords++ )
            {
                subscriber.onRecord();
                streamRecordToSubscriber( servedRecords );
                subscriber.onRecordCompleted();
            }
        }
        catch ( Throwable t )
        {
            error = t;
            servedRecords = resultSize();
        }
    }

    private long checkForOverflow( long value )
    {
        if ( value < 0 )
        {
            return Long.MAX_VALUE;
        }
        else
        {
            return value;
        }
    }
}
