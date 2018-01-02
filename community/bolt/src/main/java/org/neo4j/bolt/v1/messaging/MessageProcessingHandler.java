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
package org.neo4j.bolt.v1.messaging;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.bolt.v1.packstream.PackOutputClosedException;
import org.neo4j.bolt.v1.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

class MessageProcessingHandler implements BoltResponseHandler
{
    protected final Map<String,AnyValue> metadata = new HashMap<>();

    protected final Log log;
    protected final BoltWorker worker;
    protected final BoltResponseMessageHandler<IOException> handler;

    private Neo4jError error;
    private final Runnable onFinish;
    private boolean ignored;

    MessageProcessingHandler( BoltResponseMessageHandler<IOException> handler, Runnable onFinish, BoltWorker worker,
            Log logger )
    {
        this.handler = handler;
        this.onFinish = onFinish;
        this.worker = worker;
        this.log = logger;
    }

    @Override
    public void onStart()
    {
    }

    @Override
    public void onRecords( BoltResult result, boolean pull ) throws Exception
    {
    }

    @Override
    public void onMetadata( String key, AnyValue value )
    {
        metadata.put( key, value );
    }

    @Override
    public void markIgnored()
    {
        this.ignored = true;
    }

    @Override
    public void markFailed( Neo4jError error )
    {
        this.error = error;
    }

    @Override
    public void onFinish()
    {
        try
        {
            if ( ignored )
            {
                handler.onIgnored();
            }
            else if ( error != null )
            {
                publishError( handler, error );
            }
            else
            {
                handler.onSuccess( getMetadata() );
            }
        }
        catch ( Throwable e )
        {
            worker.halt();
            log.error( "Failed to write response to driver", e );
        }
        finally
        {
            onFinish.run();
            clearState();
        }
    }

    MapValue getMetadata()
    {
        return VirtualValues.map( metadata );
    }

    private void clearState()
    {
        error = null;
        ignored = false;
        metadata.clear();
    }

    private void publishError( BoltResponseMessageHandler<IOException> out, Neo4jError error ) throws IOException
    {
        try
        {
            if ( error.isFatal() )
            {
                out.onFatal( error.status(), error.message() );
            }
            else
            {
                out.onFailure( error.status(), error.message() );
            }
        }
        catch ( PackOutputClosedException e )
        {
            // we tried to write error back to the client and realized that the underlying channel is closed
            // log a warning, client driver might have just been stopped and closed all socket connections
            log.warn( "Unable to send error back to the client. " + e.getMessage(), error.cause() );
        }
        catch ( Throwable t )
        {
            // some unexpected error happened while writing exception back to the client
            // log it together with the original error being suppressed
            t.addSuppressed( error.cause() );
            log.error( "Unable to send error back to the client", t );
        }
    }
}
