/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.v4.runtime;

import java.time.Clock;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.bolt.v3.runtime.CypherAdapterStreamV3;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.values.AnyValue;

public class BufferedCypherAdapterStreamV4 extends CypherAdapterStreamV3
{
    private static final QueryResult.Record DONE_RECORD = () -> new AnyValue[0];
    private static final int BUFFER_SIZE = 1024;

    private final BlockingQueue<QueryResult.Record> records;
    private volatile Exception error;
    private final Thread fetchingThread = new Thread()
    {
        public void run()
        {
            try
            {
                long start = clock.millis();
                delegate.accept( row -> {
                    AnyValue[] src = row.fields();
                    AnyValue[] dest = new AnyValue[src.length];
                    System.arraycopy( src, 0, dest, 0, src.length );
                    records.put( () -> dest );
                    return true;
                } );
                addRecordStreamingTime( clock.millis() - start );
                addMetadata();

            }
            catch ( Exception e )
            {
                error = e;
            }
            finally
            {
                try{
                    records.put( DONE_RECORD );
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
            }
        }
    };

    public BufferedCypherAdapterStreamV4( QueryResult delegate, Clock clock )
    {
        super( delegate, clock );
        this.records = new LinkedBlockingQueue<>( BUFFER_SIZE );
        this.fetchingThread.start();
    }

    @Override
    public boolean handlePullRecords( final Visitor visitor, long size ) throws Exception
    {
        while ( size-- > 0 )
        {
            QueryResult.Record current = records.take();
            if ( current != DONE_RECORD )
            {
                visitor.visit( current );
            }
            // if we are at the end of the records
            if ( current == DONE_RECORD || records.peek() == DONE_RECORD )
            {
                if( error != null )
                {
                    throw error;
                }
                else
                {
                    metadata.forEach( visitor::addMetadata );
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void handleDiscardRecords( Visitor visitor ) throws Exception
    {
        // We shall also still go though all records but not sending them back to client.
        while ( records.take() != DONE_RECORD )
        {
            // drain all records
        }
        metadata.forEach( visitor::addMetadata );
    }
}
