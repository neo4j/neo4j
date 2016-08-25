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

import org.neo4j.bolt.v1.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.Record;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is responsible for routing incoming request messages to a worker
 * as well as handling outgoing response messages via appropriate handlers.
 */
public class BoltMessageRouter implements BoltRequestMessageHandler<RuntimeException>
{
    // Note that these callbacks can be used for multiple in-flight requests simultaneously, you cannot reset them
    // while there are in-flight requests.
    private final MessageProcessingHandler initHandler;
    private final MessageProcessingHandler runHandler;
    private final MessageProcessingHandler pullAllHandler;
    private final MessageProcessingHandler defaultHandler;

    private BoltWorker worker;

    public BoltMessageRouter( Log log, BoltWorker worker, BoltResponseMessageHandler<IOException> output,
                              Runnable onEachCompletedRequest )
    {
        this.initHandler = new InitHandler( output, onEachCompletedRequest, log );
        this.runHandler = new RunHandler( output, onEachCompletedRequest, log );
        this.pullAllHandler = new ResultHandler( output, onEachCompletedRequest, log );
        this.defaultHandler = new MessageProcessingHandler( output, onEachCompletedRequest, log );

        this.worker = worker;
    }

    @Override
    public void onInit( String userAgent, Map<String,Object> authToken ) throws RuntimeException
    {
        // TODO: make the client transmit the version for now it is hardcoded to -1 to ensure current behaviour
        worker.enqueue( session -> session.init( userAgent, authToken, initHandler ) );
    }

    @Override
    public void onAckFailure() throws RuntimeException
    {
        worker.enqueue( session -> session.ackFailure( defaultHandler ) );
    }

    @Override
    public void onReset() throws RuntimeException
    {
        worker.interrupt();
        worker.enqueue( session -> session.reset( defaultHandler ) );
    }

    @Override
    public void onRun( String statement, Map<String,Object> params )
    {
        worker.enqueue( session -> session.run( statement, params, runHandler ) );
    }

    @Override
    public void onDiscardAll()
    {
        worker.enqueue( session -> session.discardAll( defaultHandler ) );
    }

    @Override
    public void onPullAll()
    {
        worker.enqueue( session -> session.pullAll( pullAllHandler ) );
    }

    static class MessageProcessingHandler implements BoltResponseHandler
    {
        protected final Map<String, Object> metadata = new HashMap<>();

        // TODO: move this somewhere more sane (when modules are unified)
        static void publishError( BoltResponseMessageHandler<IOException> out, Neo4jError error )
                throws IOException
        {
            if ( !error.status().code().classification().shouldRespondToClient() )
            {
                // If not intended for client, we only return an error reference. This must
                // be cross-referenced with the log files for full error detail.
                out.onFailure( error.status(), String.format(
                        "An unexpected failure occurred, see details in the database " +
                        "logs, reference number %s.", error.reference() ) );
            }
            else
            {
                // If intended for client, we forward the message as-is.
                out.onFailure( error.status(), error.message() );
            }
        }

        protected final Log log;

        protected final BoltResponseMessageHandler<IOException> handler;

        private Neo4jError error;
        private final Runnable onFinish;
        private boolean ignored;

        MessageProcessingHandler( BoltResponseMessageHandler<IOException> handler, Runnable onFinish, Log logger )
        {
            this.handler = handler;
            this.onFinish = onFinish;
            this.log = logger;
        }

        @Override
        public void onStart()
        {
        }

        @Override
        public void onRecords( BoltResult result, boolean pull ) throws Exception
        {
            // Overridden if records are returned, therefore
            // should fail if called but not overridden.
            assert false;
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
                // TODO: we've lost the ability to communicate with the client. Shut down the session, close transactions.
                log.error( "Failed to write response to driver", e );
            }
            finally
            {
                onFinish.run();
                clearState();
            }
        }

        Map<String, Object> getMetadata()
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

    private static class InitHandler extends MessageProcessingHandler
    {
        InitHandler( BoltResponseMessageHandler<IOException> handler, Runnable onCompleted, Log log )
        {
            super( handler, onCompleted, log );
        }

    }

    private static class RunHandler extends MessageProcessingHandler
    {
        RunHandler( BoltResponseMessageHandler<IOException> handler, Runnable onCompleted, Log log )
        {
            super( handler, onCompleted, log );
        }

    }
    private static class ResultHandler extends MessageProcessingHandler
    {
        ResultHandler( BoltResponseMessageHandler<IOException> handler, Runnable onCompleted, Log log )
        {
            super( handler, onCompleted, log );
        }

        @Override
        public void onRecords( final BoltResult result, final boolean pull ) throws Exception
        {
            result.accept( new BoltResult.Visitor()
            {
                @Override
                public void visit( Record record ) throws Exception
                {
                    if ( pull )
                    {
                        handler.onRecord( record );
                    }
                }

                @Override
                public void addMetadata( String key, Object value )
                {
                    metadata.put( key, value );
                }

            } );
        }

    }
}
