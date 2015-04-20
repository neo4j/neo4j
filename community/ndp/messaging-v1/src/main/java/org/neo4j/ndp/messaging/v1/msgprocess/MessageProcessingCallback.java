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
package org.neo4j.ndp.messaging.v1.msgprocess;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.neo4j.logging.Log;
import org.neo4j.ndp.messaging.v1.MessageHandler;
import org.neo4j.ndp.runtime.Session;
import org.neo4j.ndp.runtime.internal.Neo4jError;

public class MessageProcessingCallback<T> implements Session.Callback<T,Void>
{
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
    public void result( T result, Void none ) throws Exception
    {
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
                out.handleFailureMessage( error );
            }
            else
            {
                out.handleSuccessMessage( successMetadata() );
            }
        }
        catch ( IOException e )
        {
            // Tough one to recover from, at this point, we've failed to write a response
            // back to the client. We may consider signaling the state machine about this in
            // some way, but this is left as a future exercise.
            log.error( "Failed to write response to driver", e );
        }
        finally
        {
            onCompleted.run();
            clearState();
        }
    }

    /** Allow sub-classes to override this to provide custom metadata */
    protected Map successMetadata()
    {
        return Collections.EMPTY_MAP;
    }

    private void clearState()
    {
        error = null;
        ignored = false;
    }
}
