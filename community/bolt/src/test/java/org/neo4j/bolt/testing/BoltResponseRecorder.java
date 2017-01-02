/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.testing;

import org.neo4j.bolt.v1.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.Record;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.FAILURE;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.IGNORED;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.SUCCESS;

public class BoltResponseRecorder implements BoltResponseHandler
{
    private BlockingQueue<RecordedBoltResponse> responses;
    private RecordedBoltResponse currentResponse;

    public BoltResponseRecorder()
    {
        reset();
    }

    public void reset()
    {
        responses = new LinkedBlockingQueue<>();
        currentResponse = new RecordedBoltResponse();
    }

    @Override
    public void onStart()
    {
    }

    @Override
    public void onRecords( BoltResult result, boolean pull ) throws Exception
    {
        result.accept( new BoltResult.Visitor()
        {
            @Override
            public void visit( Record record ) throws Exception
            {
                currentResponse.addRecord( record );
            }

            @Override
            public void addMetadata( String key, Object value )
            {
                currentResponse.addMetadata( key, value );
            }
        } );
    }

    @Override
    public void onMetadata( String key, Object value )
    {
        currentResponse.addMetadata( key, value );
    }

    @Override
    public void markIgnored()
    {
        currentResponse.setResponse( IGNORED );
    }

    @Override
    public void markFailed( Neo4jError error )
    {
        currentResponse.setResponse( FAILURE );
        onMetadata( "code", error.status().code().serialize() );
        onMetadata( "message", error.message() );
    }

    @Override
    public void onFinish()
    {
        if ( currentResponse.message() == null )
        {
            currentResponse.setResponse( SUCCESS );
        }
        responses.add( currentResponse );
        currentResponse = new RecordedBoltResponse();
    }

    public int responseCount()
    {
        return responses.size();
    }

    public RecordedBoltResponse nextResponse() throws InterruptedException
    {
        RecordedBoltResponse response = responses.poll( 3, SECONDS );
        assertNotNull( "No message arrived after 3s", response );
        return response;
    }

}
