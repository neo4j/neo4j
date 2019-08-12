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
package org.neo4j.bolt.testing;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.FAILURE;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.IGNORED;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.SUCCESS;
import static org.neo4j.values.storable.Values.stringOrNoValue;
import static org.neo4j.values.storable.Values.stringValue;

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
    public boolean onPullRecords( BoltResult result, long size ) throws Throwable
    {
        return hasMore( result.handleRecords( new RecordingBoltResultRecordConsumer( ), size ) );
    }

    @Override
    public boolean onDiscardRecords( BoltResult result, long size ) throws Throwable
    {
        return hasMore( result.handleRecords( new DiscardingBoltResultVisitor(), size ) );
    }

    @Override
    public void onMetadata( String key, AnyValue value )
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
        onMetadata( "code", stringValue( error.status().code().serialize() ) );
        onMetadata( "message", stringOrNoValue( error.message() ) );
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

    private boolean hasMore( boolean hasMore )
    {
        if ( hasMore )
        {
            onMetadata( "has_more", BooleanValue.TRUE );
        }
        return hasMore;
    }

    private class DiscardingBoltResultVisitor extends BoltResult.DiscardingRecordConsumer
    {
        @Override
        public void addMetadata( String key, AnyValue value )
        {
            currentResponse.addMetadata( key, value );
        }
    }

    private class RecordingBoltResultRecordConsumer implements BoltResult.RecordConsumer
    {
        private AnyValue[] anyValues;
        private int currentOffset = -1;

        @Override
        public void addMetadata( String key, AnyValue value )
        {
            currentResponse.addMetadata( key, value );
        }

        @Override
        public void beginRecord( int numberOfFields )
        {
            currentOffset = 0;
            anyValues = new AnyValue[numberOfFields];
        }

        @Override
        public void consumeField( AnyValue value )
        {
            anyValues[currentOffset++] = value;
        }

        @Override
        public void endRecord()
        {
            currentOffset = -1;
            currentResponse.addFields( anyValues );
        }

        @Override
        public void onError()
        {
            //IGNORE
        }
    }
}
