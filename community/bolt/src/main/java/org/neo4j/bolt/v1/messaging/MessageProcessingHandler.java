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
package org.neo4j.bolt.v1.messaging;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.v1.packstream.PackOutputClosedException;
import org.neo4j.bolt.v1.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

class MessageProcessingHandler implements BoltResponseHandler
{
    // Errors that are expected when the client disconnects mid-operation
    private static final Set<Status> CLIENT_MID_OP_DISCONNECT_ERRORS = new HashSet<>( Arrays.asList(
            Status.Transaction.Terminated, Status.Transaction.LockClientStopped ) );
    protected final Map<String,AnyValue> metadata = new HashMap<>();

    protected final Log log;
    protected final BoltConnection connection;
    protected final BoltResponseMessageHandler<IOException> handler;

    private Neo4jError error;
    private boolean ignored;

    MessageProcessingHandler( BoltResponseMessageHandler<IOException> handler, BoltConnection connection,
            Log logger )
    {
        this.handler = handler;
        this.connection = connection;
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
            connection.stop();
            log.error( "Failed to write response to driver", e );
        }
        finally
        {
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

    private void publishError( BoltResponseMessageHandler<IOException> out, Neo4jError error )
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
            // Can't write error to the client, because the connection is closed.
            // Very likely our error is related to the connection being closed.

            // If the error is that the transaction was terminated, then the error is a side-effect of
            // us cleaning up stuff that was running when the client disconnected. Log a warning without
            // stack trace to highlight clients are disconnecting while stuff is running:
            if ( CLIENT_MID_OP_DISCONNECT_ERRORS.contains( error.status() ) )
            {
                log.warn( "Client %s disconnected while query was running. Session has been cleaned up. " +
                          "This can be caused by temporary network problems, but if you see this often, " +
                          "ensure your applications are properly waiting for operations to complete before exiting.",
                        e.clientAddress() );
                return;
            }

            // If the error isn't that the tx was terminated, log it to the console for debugging. It's likely
            // there are other "ok" errors that we can whitelist into the conditional above over time.
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
