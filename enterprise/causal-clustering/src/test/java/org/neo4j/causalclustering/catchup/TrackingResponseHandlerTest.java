/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup;

import org.junit.Test;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TrackingResponseHandlerTest
{
    @Test
    public void shouldCompleteSignalExceptionallyOnInactive()
    {
        // given
        TrackingResponseHandler trackingResponseHandler = new TrackingResponseHandler( new CatchUpResponseAdaptor(), Clock.systemUTC() );
        CompletableFuture<Object> future = new CompletableFuture<>();
        trackingResponseHandler.setResponseHandler( new CatchUpResponseAdaptor(), future );

        // when
        trackingResponseHandler.onChannelInactive();

        // then
        assertTrue( future.isCompletedExceptionally() );
        Throwable cause = null;
        try
        {
            future.get();
        }
        catch ( InterruptedException e )
        {
            fail();
        }
        catch ( ExecutionException e )
        {
            cause = e.getCause();
        }
        assertNotNull( cause );
        assertEquals( IllegalStateException.class, cause.getClass() );
        assertEquals( "Channel inactive", cause.getMessage() );
    }

    @Test
    public void shouldCompleteSignalExceptionallyOnException()
    {
        // given
        TrackingResponseHandler trackingResponseHandler = new TrackingResponseHandler( new CatchUpResponseAdaptor(), Clock.systemUTC() );
        CompletableFuture<Object> future = new CompletableFuture<>();
        trackingResponseHandler.setResponseHandler( new CatchUpResponseAdaptor(), future );

        IllegalArgumentException error = new IllegalArgumentException( "error" );

        // when
        trackingResponseHandler.onException( error );

        // then
        assertTrue( future.isCompletedExceptionally() );
        Throwable cause = null;
        try
        {
            future.get();
        }
        catch ( InterruptedException e )
        {
            fail();
        }
        catch ( ExecutionException e )
        {
            cause = e.getCause();
        }
        assertNotNull( cause );
        assertEquals( error, cause );
    }
}
