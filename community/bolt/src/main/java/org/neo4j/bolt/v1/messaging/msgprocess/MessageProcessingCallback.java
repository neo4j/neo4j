/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.bolt.v1.messaging.msgprocess;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.neo4j.logging.Log;
import org.neo4j.bolt.v1.messaging.MessageHandler;
import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.internal.Neo4jError;

public class MessageProcessingCallback<T> extends Session.Callback.Adapter<T,Void>
{
    // TODO: move this somewhere more sane (when modules are unified)
    public static void publishError( MessageHandler<IOException> out, Neo4jError error )
            throws IOException
    {
        if ( error.status().code().classification().publishable() )
        {
            // If publishable, we forward the message as-is to the user.
            out.handleFailureMessage( error.status(), error.message() );
        }
        else
        {
            // If not publishable, we only return an error reference to the user. This must
            // be cross-referenced with the log files for full error detail. This feature
            // exists to improve security so that sensitive information is not leaked.
            out.handleFailureMessage( error.status(), String.format(
                    "An unexpected failure occurred, see details in the database " +
                    "logs, reference number %s.", error.reference() ) );
        }
    }

    protected final Log log;

    protected MessageHandler<IOException> out;

    private Neo4jError error;
    private Runnable onCompleted;
    private boolean ignored;

    public MessageProcessingCallback( Log logger )
    {
        this.log = logger;
    }

    public MessageProcessingCallback reset(
            MessageHandler<IOException> out,
            Runnable onCompleted )
    {
        this.out = out;
        this.onCompleted = onCompleted;
        clearState();
        return this;
    }

    @Override
    public void failure( Neo4jError err, Void none )
    {
        this.error = err;
    }

    @Override
    public void ignored( Void none )
    {
        this.ignored = true;
    }

    @Override
    public void completed( Void none )
    {
        try
        {
            if ( ignored )
            {
                out.handleIgnoredMessage();
            }
            else if ( error != null )
            {
                publishError( out, error );
            }
            else
            {
                out.handleSuccessMessage( successMetadata() );
            }
        }
        catch ( Throwable e )
        {
            // TODO: we've lost the ability to communicate with the client. Shut down the session, close transactions.
            log.error( "Failed to write response to driver", e );
        }
        finally
        {
            onCompleted.run();
            clearState();
        }
    }

    /** Allow sub-classes to override this to provide custom metadata */
    protected Map<String,Object> successMetadata()
    {
        return Collections.emptyMap();
    }

    protected void clearState()
    {
        error = null;
        ignored = false;
    }
}
