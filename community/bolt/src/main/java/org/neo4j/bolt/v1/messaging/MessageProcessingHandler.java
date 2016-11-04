/*
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
package org.neo4j.bolt.v1.messaging;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.bolt.v1.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.logging.Log;

class MessageProcessingHandler implements BoltResponseHandler
{
    protected final Map<String,Object> metadata = new HashMap<>();

    // TODO: move this somewhere more sane (when modules are unified)
    static void publishError( BoltResponseMessageHandler<IOException> out, Neo4jError error )
            throws IOException
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
    public void onMetadata( String key, Object value )
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

    Map<String,Object> getMetadata()
    {
        return metadata;
    }

    void clearState()
    {
        error = null;
        ignored = false;
        metadata.clear();
    }
}
