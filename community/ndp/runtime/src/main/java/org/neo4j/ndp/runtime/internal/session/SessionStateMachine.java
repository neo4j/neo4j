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
package org.neo4j.ndp.runtime.internal.session;

import java.util.Map;
import java.util.UUID;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.logging.Log;
import org.neo4j.ndp.runtime.Session;
import org.neo4j.ndp.runtime.StatementMetadata;
import org.neo4j.ndp.runtime.internal.ErrorTranslator;
import org.neo4j.ndp.runtime.internal.Neo4jError;
import org.neo4j.ndp.runtime.internal.StatementRunner;
import org.neo4j.stream.RecordStream;

/**
 * State-machine based implementation of {@link SessionState}. With this approach,
 * the discrete states a session can be in are explicitly denoted. Each state describes which actions from the context
 * interface are legal given that particular state.
 */
public class SessionStateMachine implements Session, SessionState
{
    /**
     * The session state machine, this is the heart of how a session operates. This enumerates the various discrete
     * states a session can be in, and describes how it behaves in those states.
     */
    enum State
    {
        /**
         * No open transaction, no open result.
         */
        IDLE
        {
            @Override
            public State beginTransaction( SessionStateMachine ctx )
            {
                assert ctx.currentTransaction == null;
                ctx.implicitTransaction = false;
                ctx.currentTransaction = ctx.db.beginTx();
                return IN_TRANSACTION;
            }

            @Override
            public State runStatement( SessionStateMachine ctx, String statement, Map<String, Object> params )
            {
                try
                {
                    ctx.currentResult = ctx.statementRunner.run( ctx, statement, params );
                    ctx.result( ctx.currentStatementMetadata );
                    return STREAM_OPEN;
                }
                catch( Throwable e )
                {
                    return error(ctx, e);
                }
            }

            @Override
            public State beginImplicitTransaction( SessionStateMachine ctx )
            {
                assert ctx.currentTransaction == null;
                ctx.implicitTransaction = true;
                ctx.currentTransaction = ctx.db.beginTx();
                return IN_TRANSACTION;
            }

        },

        /**
         * Open transaction, no open stream
         *
         * This is when the client has explicitly requested a transaction to be opened.
         */
        IN_TRANSACTION
        {
            @Override
            public State runStatement( SessionStateMachine ctx, String statement, Map<String, Object> params )
            {
                return IDLE.runStatement( ctx, statement, params);
            }

            @Override
            public State commitTransaction( SessionStateMachine ctx )
            {
                try
                {
                    ctx.currentTransaction.success();
                    ctx.currentTransaction.close();
                }
                catch(Throwable e)
                {
                    return error(ctx, e);
                }
                finally
                {
                    ctx.currentTransaction = null;
                }
                return IDLE;
            }

            @Override
            public State rollbackTransaction( SessionStateMachine ctx )
            {
                try
                {
                    Transaction tx = ctx.currentTransaction;
                    ctx.currentTransaction = null;

                    tx.failure();
                    tx.close();
                    return IDLE;
                }
                catch(Throwable e)
                {
                    return error(ctx, e);
                }
            }
        },

        /**
         * A result stream is ready for consumption, there may or may not be an open transaction.
         */
        STREAM_OPEN
        {
            @Override
            public State pullAll( SessionStateMachine ctx )
            {
                try
                {
                    ctx.result( ctx.currentResult );
                    return discardAll( ctx );
                }
                catch(Throwable e)
                {
                    return error(ctx, e);
                }
            }

            @Override
            public State discardAll( SessionStateMachine ctx )
            {
                try
                {
                    ctx.currentResult.close();

                    if( !ctx.hasTransaction() )
                    {
                        return IDLE;
                    }
                    else if ( ctx.implicitTransaction )
                    {
                        return IN_TRANSACTION.commitTransaction( ctx );
                    }
                    else
                    {
                        return IN_TRANSACTION;
                    }
                }
                catch(Throwable e)
                {
                    return error(ctx, e);
                }
                finally
                {
                    ctx.currentResult = null;
                }
            }

        },

        /** An error has occurred, client must acknowledge it before anything else is allowed. */
        ERROR
        {
            @Override
            public State acknowledgeError( SessionStateMachine ctx )
            {
                return IDLE;
            }

            @Override
            protected State onNoImplementation( SessionStateMachine ctx, String command )
            {
                ctx.ignored();
                return ERROR;
            }
        },

        /** The state machine is permanently stopped. */
        STOPPED
        {
            @Override
            public State halt( SessionStateMachine ctx )
            {
                throw new IllegalStateException( "No operations allowed, session has been closed." );
            }
        }
        ;

        // Operations that a session can perform. Individual states override these if they want to support them.

        public State runStatement( SessionStateMachine ctx, String statement, Map<String, Object> params )
        {
            return onNoImplementation( ctx, "running a statement" );
        }

        public State pullAll( SessionStateMachine ctx )
        {
            return onNoImplementation( ctx, "pulling full stream" );
        }

        public State discardAll( SessionStateMachine ctx )
        {
            return onNoImplementation( ctx, "discarding remainder of stream" );
        }

        public State commitTransaction( SessionStateMachine ctx )
        {
            return onNoImplementation( ctx, "committing transaction" );
        }

        public State rollbackTransaction( SessionStateMachine ctx )
        {
            return onNoImplementation( ctx, "rolling back transaction" );
        }

        public State beginImplicitTransaction( SessionStateMachine ctx )
        {
            return onNoImplementation( ctx, "beginning implicit transaction" );
        }

        public State beginTransaction( SessionStateMachine ctx )
        {
            return onNoImplementation( ctx, "beginning implicit transaction" );
        }

        public State acknowledgeError( SessionStateMachine ctx )
        {
            return onNoImplementation( ctx, "acknowledging an error" );
        }

        protected State onNoImplementation( SessionStateMachine ctx, String command )
        {
            String msg = "'" + command + "' cannot be done when a session is in the '" + ctx.state.name() + "' state.";
            return error(ctx, new Neo4jError( Status.Request.Invalid, msg ));
        }

        public State halt( SessionStateMachine ctx )
        {
            if(ctx.currentTransaction != null)
            {
                try
                {
                    ctx.currentTransaction.close();
                }
                catch( Throwable e)
                {
                    ctx.error( ctx.errTrans.translate( e ) );
                }
            }
            return STOPPED;
        }

        State error( SessionStateMachine ctx, Throwable err )
        {
            return error( ctx, ctx.errTrans.translate( err ) );
        }

        State error( SessionStateMachine ctx, Neo4jError err )
        {
            if ( ctx.hasTransaction() )
            {
                try
                {
                    ctx.currentTransaction.failure();
                    ctx.currentTransaction.close();
                }
                catch ( Throwable t )
                {
                    ctx.log.error( "While handling '" + err.status() + "', a second failure occurred when rolling " +
                                   "back transaction: " + t.getMessage(), t );
                }
                finally
                {
                    ctx.currentTransaction = null;
                }
            }
            ctx.error( err );
            return ERROR;
        }
    }

    private final GraphDatabaseService db;
    private final StatementRunner statementRunner;
    private final ErrorTranslator errTrans;
    private final Log log;
    private final String id;

    /** A re-usable statement metadata instance that always represents the currently running statement */
    private final StatementMetadata currentStatementMetadata = new StatementMetadata()
    {
        @Override
        public String[] fieldNames()
        {
            return currentResult.fieldNames();
        }
    };

    /** The current session state */
    private State state = State.IDLE;

    /** The current pending result, if present */
    private RecordStream currentResult;

    /** The current transaction, if present */
    private Transaction currentTransaction;

    /** Callback poised to receive the next response */
    private Callback currentCallback;

    /** Callback attachment */
    private Object currentAttachment;
    
    private ThreadToStatementContextBridge txBridge;

    /**
     * Flag to indicate whether the current transaction, if present, is implicit. An
     * implicit transaction is one not explicitly requested by the user but implicitly
     * added to wrap a statement for execution. An implicit transaction will always
     * commit when its result is closed.
     */
    private boolean implicitTransaction = false;

    // Note: We shouldn't depend on GDB like this, I think. Better to define an SPI that we can shape into a spec
    // for exactly the kind of underlying support the state machine needs.
    public SessionStateMachine( GraphDatabaseService db, ThreadToStatementContextBridge txBridge, StatementRunner engine, Log log )
    {
        this.db = db;
        this.txBridge = txBridge;
        this.statementRunner = engine;
        this.errTrans = new ErrorTranslator( log );
        this.log = log;
        this.id = UUID.randomUUID().toString();
    }

    @Override
    public String key()
    {
        return id;
    }

    @Override
    public <A> void run( String statement, Map<String, Object> params, A attachment, Callback<StatementMetadata,A> callback )
    {
        before( attachment, callback );
        try
        {
            state = state.runStatement( this, statement, params );
        } finally { after(); }
    }

    @Override
    public <A> void pullAll( A attachment, Callback<RecordStream,A> callback )
    {
        before( attachment, callback );
        try
        {
            state = state.pullAll( this );
        } finally { after(); }
    }

    @Override
    public <A> void discardAll( A attachment, Callback<Void,A> callback )
    {
        before( attachment, callback );
        try
        {
            state = state.discardAll( this );
        } finally { after(); }
    }

    @Override
    public <A> void acknowledgeFailure( A attachment, Callback<Void,A> callback )
    {
        before( attachment, callback );
        try
        {
            state = state.acknowledgeError( this );
        } finally { after(); }
    }

    @Override
    public void close()
    {
        before( null, null );
        try
        {
            state = state.halt( this );
        } finally { after(); }
    }

    @Override
    public void beginImplicitTransaction()
    {
        state = state.beginImplicitTransaction( this );
    }

    @Override
    public void beginTransaction()
    {
        state = state.beginTransaction( this );
    }

    @Override
    public void commitTransaction()
    {
        state = state.commitTransaction( this );
    }

    @Override
    public void rollbackTransaction()
    {
        state = state.rollbackTransaction( this );
    }

    @Override
    public Statement statement()
    {
        return txBridge.get();
    }

    @Override
    public boolean hasTransaction()
    {
        return currentTransaction != null;
    }

    public State state()
    {
        return state;
    }

    @Override
    public String toString()
    {
        return "Session[" + id + "," + state.name() + "]";
    }

    /**
     * Set the callback to receive the next response. This will receive one completion or one failure, and then be
     * detached again. This exists both to ensure that each callback only gets called once, as well as to avoid
     * repeating the callback and attachments in every method signature in the state machine.
     */
    private void before( Object attachment, Callback cb )
    {
        if ( hasTransaction() )
        {
            txBridge.bindTransactionToCurrentThread( (TopLevelTransaction) currentTransaction );
        }
        assert this.currentCallback == null;
        assert this.currentAttachment == null;
        this.currentCallback = cb;
        this.currentAttachment = attachment;
    }

    /** Signal to the currently attached client callback that the request has been processed */
    @SuppressWarnings( "unchecked" )
    private void after()
    {
        try
        {
            if ( currentCallback != null )
            {
                try
                {
                    currentCallback.completed( currentAttachment );
                }
                finally
                {
                    currentCallback = null;
                    currentAttachment = null;
                }
            }
        }
        finally
        {
            if ( hasTransaction() )
            {
                txBridge.unbindTransactionFromCurrentThread();
            }
        }
    }

    /** Forward an error to the currently attached callback */
    private void error( Neo4jError err )
    {
        if(currentCallback != null)
        {
            currentCallback.failure( err, currentAttachment );
        }
    }

    /** Forward a result to the currently attached callback */
    private void result( Object result ) throws Exception
    {
        if(currentCallback != null)
        {
            currentCallback.result( result, currentAttachment );
        }
    }

    /**
     * A message is being ignored, because the state machine is waiting for an error to be acknowledged before it
     * resumes processing.
     */
    private void ignored()
    {
        if(currentCallback != null)
        {
            currentCallback.ignored(currentAttachment);
        }
    }
}
