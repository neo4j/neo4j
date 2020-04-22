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
package org.neo4j.bolt.runtime.statemachine.impl;

import java.io.IOException;

import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.exceptions.CypherExecutionException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;

public class BoltAdapterSubscriber implements QuerySubscriber
{
    private BoltResult.RecordConsumer recordConsumer;
    private Throwable error;
    private QueryStatistics statistics;
    private int numberOfFields;

    @Override
    public void onResult( int numberOfFields )
    {
        this.numberOfFields = numberOfFields;
    }

    @Override
    public void onRecord() throws IOException
    {
        recordConsumer.beginRecord( numberOfFields );
    }

    @Override
    public void onField( int offset, AnyValue value ) throws IOException
    {
        recordConsumer.consumeField( value );
    }

    @Override
    public void onRecordCompleted() throws Exception
    {
        recordConsumer.endRecord();
    }

    @Override
    public void onError( Throwable throwable ) throws IOException
    {
        if ( this.error == null )
        {
            this.error = throwable;
        }
        //error might occur before the recordConsumer was initialized
        if ( recordConsumer != null )
        {
            recordConsumer.onError();
        }
    }

    @Override
    public void onResultCompleted( QueryStatistics statistics )
    {
        this.statistics = statistics;
    }

    public QueryStatistics queryStatistics()
    {
        return statistics;
    }

    public void setRecordConsumer( BoltResult.RecordConsumer recordConsumer )
    {
        this.recordConsumer = recordConsumer;
    }

    public void assertSucceeded() throws KernelException
    {
        if ( error != null )
        {
            if ( error instanceof KernelException )
            {
                throw (KernelException) error;
            }
            else if ( error instanceof Status.HasStatus )
            {
                throw new QueryExecutionKernelException( (Throwable & Status.HasStatus) error );
            }
            else
            {
                throw new QueryExecutionKernelException( new CypherExecutionException( error.getMessage(), error ) );
            }
        }
    }
}
