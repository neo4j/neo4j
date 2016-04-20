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
package org.neo4j.bolt.v1.runtime.internal;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.StatementMetadata;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.bolt.v1.runtime.spi.StatementRunner;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.udc.UsageData;

import static java.lang.String.format;
import static org.neo4j.kernel.api.KernelTransaction.Type.explicit;
import static org.neo4j.kernel.api.KernelTransaction.Type.implicit;
import static org.neo4j.kernel.api.exceptions.Status.Security.CredentialsExpired;

/**
 * State-machine based implementation of {@link Session}. With this approach,
 * the discrete states a session can be in are explicit. Each state describes which actions from the context
 * interface are legal given that particular state, and how those actions behave given the current state.
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
         * Before the session has been initialized.
         */
        UNINITIALIZED
                {
                    @Override
                    public State init( SessionStateMachine ctx, String clientName, Map<String,Object> authToken )
                    {
                        try
                        {
                            AuthenticationResult authResult = ctx.spi.authenticate( authToken );
                            ctx.accessMode = authResult.getAccessMode();
                            ctx.credentialsExpired = authResult.credentialsExpired();
                            ctx.result( authResult.credentialsExpired() );
                            ctx.spi.udcRegisterClient( clientName );
                            ctx.setQuerySourceFromClientNameAndPrincipal( clientName, authToken.get( Authentication.PRINCIPAL ) );
                            return IDLE;
                        }
                        catch ( AuthenticationException e )
                        {
                            return error( ctx, new Neo4jError( e.status(), e.getMessage(), e ) );
                        }
                        catch ( Throwable e )
                        {
                            return error( ctx, e );
                        }
                    }

                    @Override
                    protected State onNoImplementation( SessionStateMachine ctx, String command )
                    {
                        ctx.error( new Neo4jError( Status.Request.Invalid, "No operations allowed until you send an " +
                                "INIT message." ) );
                        return halt( ctx );
                    }
                },

        /**
         * No open transaction, no open result.
         */
        IDLE
                {
                    @Override
                    public State beginTransaction( SessionStateMachine ctx )
                    {
                        assert ctx.currentTransaction == null;
                        ctx.currentTransaction = ctx.spi.beginTransaction( explicit, ctx.accessMode );
                        return IN_TRANSACTION;
                    }

                    @Override
                    public State runStatement( SessionStateMachine ctx, String statement, Map<String,Object> params )
                    {
                        try
                        {
                            ctx.currentResult = ctx.spi.run( ctx, statement, params );
                            ctx.result( ctx.currentStatementMetadata );
                            //if the call to run failed we must remain in state ERROR
                            if ( ctx.state == ERROR )
                            {
                                return ERROR;
                            }
                            else
                            {
                                return STREAM_OPEN;
                            }
                        }
                        catch ( Throwable e )
                        {
                            return error( ctx, e );
                        }
                    }

                    @Override
                    public State beginImplicitTransaction( SessionStateMachine ctx )
                    {
                        assert ctx.currentTransaction == null;
                        // NOTE: If we move away from doing implicit transactions this
                        // way, we need a different way to kill statements running in implicit
                        // transactions, because we do that by calling #terminate() on this tx.
                        ctx.currentTransaction = ctx.spi.beginTransaction( implicit, ctx.accessMode );
                        return IN_TRANSACTION;
                    }

                    @Override
                    public State reset( SessionStateMachine ctx )
                    {
                        return IDLE;
                    }

                    @Override
                    public State rollbackTransaction( SessionStateMachine ctx )
                    {
                        return error( ctx, new Neo4jError( Status.Request.Invalid,
                                "rollback cannot be done when there is no open transaction in the session." ) );
                    }
                },

        /**
         * Open transaction, no open stream
         * <p>
         * This is when the client has explicitly requested a transaction to be opened.
         */
        IN_TRANSACTION
                {
                    @Override
                    public State runStatement( SessionStateMachine ctx, String statement, Map<String,Object> params )
                    {
                        return IDLE.runStatement( ctx, statement, params );
                    }

                    @Override
                    public State commitTransaction( SessionStateMachine ctx )
                    {
                        try
                        {
                            KernelTransaction tx = ctx.currentTransaction;
                            ctx.currentTransaction = null;

                            tx.success();
                            tx.close();
                        }
                        catch ( Throwable e )
                        {
                            return error( ctx, e );
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
                            KernelTransaction tx = ctx.currentTransaction;
                            ctx.currentTransaction = null;

                            tx.failure();
                            tx.close();
                            return IDLE;
                        }
                        catch ( Throwable e )
                        {
                            return error( ctx, e );
                        }
                    }

                    @Override
                    public State reset( SessionStateMachine ctx )
                    {
                        return rollbackTransaction( ctx );
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
                        catch ( Throwable e )
                        {
                            return error( ctx, e );
                        }
                    }

                    @Override
                    public State discardAll( SessionStateMachine ctx )
                    {
                        try
                        {
                            ctx.currentResult.close();

                            if ( !ctx.hasTransaction() )
                            {
                                return IDLE;
                            }
                            else if ( ctx.currentTransaction.transactionType() == implicit )
                            {
                                return IN_TRANSACTION.commitTransaction( ctx );
                            }
                            else
                            {
                                return IN_TRANSACTION;
                            }
                        }
                        catch ( Throwable e )
                        {
                            return error( ctx, e );
                        }
                        finally
                        {
                            ctx.currentResult = null;
                        }
                    }

                    @Override
                    public State reset( SessionStateMachine ctx )
                    {
                        // Do an extra reset, since discardAll may put us
                        // in the IN_TRANSACTION state
                        return discardAll( ctx ).reset( ctx );
                    }

                },

        /** An error has occurred, client must acknowledge it before anything else is allowed. */
        ERROR
                {
                    @Override
                    public State reset( SessionStateMachine ctx )
                    {
                        // There may still be a transaction open, so do
                        // an extra reset on the outcome of ackFailure to ensure we go
                        // to idle state.
                        return ackFailure( ctx ).reset( ctx );
                    }

                    @Override
                    public State ackFailure( SessionStateMachine ctx )
                    {
                        if( ctx.hasTransaction() )
                        {
                            return IN_TRANSACTION;
                        }
                        return IDLE;
                    }

                    @Override
                    protected State onNoImplementation( SessionStateMachine ctx, String command )
                    {
                        ctx.ignored();
                        return ERROR;
                    }
                },

        /**
         * The state machine is in a temporary INTERRUPT state, and will ignore
         * all messages until a RESET is received.
         */
        INTERRUPTED
                {
                    /**
                     * When we are in the interrupted state, we need RESET messages
                     * to clear that state.
                     */
                    @Override
                    public State reset( SessionStateMachine ctx )
                    {
                        // If > 0 to guard against bugs making the counter negative
                        if( ctx.interruptCounter.get() > 0 )
                        {
                            if( ctx.interruptCounter.decrementAndGet() > 0 )
                            {
                                // This happens when the user sends multiple
                                // interrupts at the same time, we now demand
                                // an equivalent number of RESET until we go back
                                // to IDLE.
                                ctx.ignored();
                                return INTERRUPTED;
                            }
                        }
                        return IDLE;
                    }


                    @Override
                    public State interrupt( SessionStateMachine ctx )
                    {
                        return INTERRUPTED;
                    }

                    @Override
                    protected State onNoImplementation( SessionStateMachine ctx, String command )
                    {
                        ctx.ignored();
                        return INTERRUPTED;
                    }
                },

        /** The state machine is permanently stopped. */
        STOPPED
                {
                    @Override
                    public State halt( SessionStateMachine ctx )
                    {
                        return STOPPED;
                    }

                    @Override
                    protected State onNoImplementation( SessionStateMachine ctx, String command )
                    {
                        ctx.ignored();
                        return STOPPED;
                    }
                };

        // Operations that a session can perform. Individual states override these if they want to support them.

        public State init( SessionStateMachine ctx, String clientName, Map<String,Object> authToken )
        {
            return onNoImplementation( ctx, "initializing the session" );
        }

        public State runStatement( SessionStateMachine ctx, String statement, Map<String,Object> params )
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

        public State reset( SessionStateMachine ctx )
        {
            return onNoImplementation( ctx, "resetting the current session" );
        }

        public State ackFailure( SessionStateMachine ctx )
        {
            return onNoImplementation( ctx, "acknowledging a failure" );
        }

        /**
         * If the session has been interrupted, this will be invoked before *each*
         * message that is processed after interruption, until the interrupt counter
         * is reset back to 0. This exists to ensure we cleanly reset any current
         * state, meaning the default implementation is the same as reset.
         */
        public State interrupt( SessionStateMachine ctx )
        {
            reset( ctx );
            return INTERRUPTED;
        }

        protected State onNoImplementation( SessionStateMachine ctx, String command )
        {
            String msg = "'" + command + "' cannot be done when a session is in the '" + ctx.state.name() + "' state.";
            return error( ctx, new Neo4jError( Status.Request.Invalid, msg ) );
        }

        public State halt( SessionStateMachine ctx )
        {
            if ( ctx.currentTransaction != null )
            {
                try
                {
                    ctx.currentTransaction.close();
                }
                catch ( Throwable e )
                {
                    ctx.error( Neo4jError.from( e ) );
                }
            }
            return STOPPED;
        }

        State error( SessionStateMachine ctx, Throwable err )
        {
            if( err instanceof AuthorizationViolationException &&
                ctx.credentialsExpired )
            {
                // TODO: This is *way* too high up the stack to create this message, this should
                //       happen much further down.
                return error( ctx, new Neo4jError( CredentialsExpired,
                        String.format("The credentials you provided were valid, but must be changed before you can " +
                        "use this instance. If this is the first time you are using Neo4j, this is to " +
                        "ensure you are not using the default credentials in production. If you are not " +
                        "using default credentials, you are getting this message because an administrator " +
                        "requires a password change.%n" +
                        "Changing your password is easy to do via the Neo4j Browser.%n" +
                        "If you are connecting via a shell or programmatically via a driver, " +
                        "just issue a `CALL dbms.changePassword('new password')` statement in the current " +
                        "session, and then restart your driver with the new password configured."),
                        err ) );
            }
            return error( ctx, Neo4jError.from( err ) );
        }

        State error( SessionStateMachine ctx, Neo4jError err )
        {
            ctx.spi.reportError( err );
            State outcome = ERROR;
            if ( ctx.hasTransaction() )
            {
                switch( ctx.currentTransaction.transactionType() )
                {
                case explicit:
                    ctx.currentTransaction.failure();
                    break;
                case implicit:
                    try
                    {
                        ctx.currentTransaction.failure();
                        ctx.currentTransaction.close();
                    }
                    catch ( Throwable t )
                    {
                        ctx.spi.reportError( "While handling '" + err.status() + "', a second failure occurred when " +
                                "rolling back transaction: " + t.getMessage(), t );
                    }
                    finally
                    {
                        ctx.currentTransaction = null;
                    }
                    break;
                }
            }
            ctx.error( err );
            return outcome;
        }
    }

    private final String id = UUID.randomUUID().toString();

    /** A re-usable statement metadata instance that always represents the currently running statement */
    private final StatementMetadata currentStatementMetadata = new StatementMetadata()
    {
        @Override
        public String[] fieldNames()
        {
            return currentResult.fieldNames();
        }
    };

    /**
     * This is incremented each time {@link #interrupt()} is called,
     * and decremented each time a {@link #reset(Object, Callback)} message
     * arrives. When this is above 0, all messages will be ignored.
     * This way, when a reset message arrives on the network, interrupt
     * can be called to "purge" all the messages ahead of the reset message.
     */
    private final AtomicInteger interruptCounter = new AtomicInteger();

    /** The current session state */
    private State state = State.UNINITIALIZED;

    /** The current pending result, if present */
    private RecordStream currentResult;

    /** The current transaction, if present */
    private KernelTransaction currentTransaction;

    /** The current query source, if initialized */
    private String currentQuerySource;

    /** Callback poised to receive the next response */
    private Callback currentCallback;

    /** Callback attachment */
    private Object currentAttachment;

    /** The current session auth state to be used for starting transactions */
    private AccessMode accessMode;

    /**
     * If the current user has provided valid but needs-to-be-changed credentials,
     * this flag gets set. This is not awesome - it'd be better to have a special
     * access mode for this state, that would help disambiguate from being unauthenticated
     * as well. Did things this way to minimize risk of introducing bugs this late
     * in the 3.0 cycle. A further note towards adding a special AccessMode is that
     * we need to set things up to change access mode anyway whenever the user changes
     * credentials or is upgraded. As it is now, a new session needs to be created.
     */
    private boolean credentialsExpired;

    /** These are the "external" actions the state machine can take */
    private final SPI spi;

    /**
     * This SPI encapsulates the "external" actions the state machine can take.
     * It exists for three reasons:
     * 1) It makes it very clear what side-effects the SSM can have
     * 2) It decouples the SSM from the actual components performing these operations
     * 3) It makes it *much* easier to test the SSM without having to re-implement
     *    the whole database as mocks.
     *
     * If you are adding new functionality to the SSM where the new function needs
     * to reach out to some component outside the SSM, please add it here. And when
     * you do, please consider the law of demeter - if you are simply adding
     * "getQueryEngine" to the SPI, you're doing it wrong, then we might as well
     * have the full components as fields.
     */
    interface SPI
    {
        String connectionDescriptor();
        void reportError( Neo4jError err );
        void reportError( String message, Throwable cause );
        KernelTransaction beginTransaction( KernelTransaction.Type type, AccessMode mode );
        void bindTransactionToCurrentThread( KernelTransaction tx );
        void unbindTransactionFromCurrentThread();
        RecordStream run( SessionStateMachine ctx, String statement, Map<String, Object> params )
                throws KernelException;
        AuthenticationResult authenticate( Map<String, Object> authToken ) throws AuthenticationException;
        void udcRegisterClient( String clientName );
        Statement currentStatement();
    }
    public SessionStateMachine( String connectionDescriptor, UsageData usageData, GraphDatabaseFacade db, ThreadToStatementContextBridge txBridge,
            StatementRunner engine, LogService logging, Authentication authentication )
    {
        this( new StandardStateMachineSPI( connectionDescriptor, usageData, db, engine, logging, authentication, txBridge ));
    }
    public SessionStateMachine( SPI spi )
    {
        this.spi = spi;
        this.accessMode = AccessMode.Static.NONE;
    }

    @Override
    public String key()
    {
        return id;
    }

    public String connectionDescriptor()
    {
        return spi.connectionDescriptor();
    }

    private String querySource()
    {
        return currentQuerySource;
    }

    private void setQuerySourceFromClientNameAndPrincipal( String clientName, Object principal )
    {
        String principalName = principal == null ? "null" : principal.toString();
        currentQuerySource = format( "bolt\t%s\t%s\t%s>", principalName, clientName, connectionDescriptor() );
    }

    @Override
    public <A> void init( String clientName, Map<String,Object> authToken, A attachment, Callback<Boolean,A> callback )
    {
        before( attachment, callback );
        try
        {
            state = state.init( this, clientName, authToken );
        }
        finally { after(); }
    }

    @Override
    public <A> void run( String statement, Map<String,Object> params, A attachment,
            Callback<StatementMetadata,A> callback )
    {
        before( attachment, callback );
        try
        {
            state = state.runStatement( this, statement, params );
        }
        finally { after(); }
    }

    @Override
    public <A> void pullAll( A attachment, Callback<RecordStream,A> callback )
    {
        before( attachment, callback );
        try
        {
            state = state.pullAll( this );
        }
        finally { after(); }
    }

    @Override
    public <A> void discardAll( A attachment, Callback<Void,A> callback )
    {
        before( attachment, callback );
        try
        {
            state = state.discardAll( this );
        }
        finally { after(); }
    }

    @Override
    public <A> void ackFailure( A attachment, Callback<Void,A> callback )
    {
        before( attachment, callback );
        try
        {
            state = state.ackFailure( this );
        }
        finally { after(); }
    }

    @Override
    public <A> void reset( A attachment, Callback<Void,A> callback )
    {
        before( attachment, callback );
        try
        {
            state = state.reset( this );
        }
        finally { after(); }
    }

    @Override
    public void interrupt()
    {
        // NOTE: This is a side-channel method call. You *cannot*
        //       mutate any of the regular state in the state machine
        //       from inside this method, it WILL lead to race conditions.
        //       Imagine this is always called from a separate thread, while
        //       the main session worker thread is actively working on mutating
        //       fields on the session.
        interruptCounter.incrementAndGet();

        // If there is currently a transaction running, terminate it
        KernelTransaction tx = this.currentTransaction;
        if(tx != null)
        {
            tx.markForTermination();
        }
    }

    @Override
    public void close()
    {
        before( null, null );
        try
        {
            state = state.halt( this );
        }
        finally { after(); }
    }

    // Below are methods used from within the state machine, to alter state while its executing an action

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
    public boolean hasTransaction()
    {
        return currentTransaction != null;
    }

    @Override
    public QuerySession createSession( GraphDatabaseQueryService service, PropertyContainerLocker locker )
    {
        InternalTransaction transaction = service.beginTransaction( implicit, accessMode );
        Neo4jTransactionalContext transactionalContext =
                new Neo4jTransactionalContext( service, transaction, spi.currentStatement(), locker );
        return new BoltQuerySession( transactionalContext, querySource() );
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
        if ( cb != null )
        {
            cb.started( attachment );
        }

        if( interruptCounter.get() > 0 )
        {
            // Force into interrupted state. This is how we 'discover'
            // that `interrupt` has been called.
            // First reset, so we clean up any open resources
            state = state.interrupt( this );
        }

        if ( hasTransaction() )
        {
            spi.bindTransactionToCurrentThread( currentTransaction );
        }
        assert this.currentCallback == null;
        assert this.currentAttachment == null;
        this.currentCallback = cb;
        this.currentAttachment = attachment;
    }

    /** Signal to the currently attached client callback that the request has been processed */
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
                spi.unbindTransactionFromCurrentThread();
            }
        }
    }

    /** Forward an error to the currently attached callback */
    private void error( Neo4jError err )
    {
        if ( err.status().code().classification() == Status.Classification.DatabaseError )
        {
            spi.reportError( err );
        }

        if ( currentCallback != null )
        {
            currentCallback.failure( err, currentAttachment );
        }
    }

    /** Forward a result to the currently attached callback */
    private void result( Object result ) throws Exception
    {
        if ( currentCallback != null )
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
        if ( currentCallback != null )
        {
            currentCallback.ignored( currentAttachment );
        }
    }

    private class BoltQuerySession extends QuerySession
    {
        private final String querySource;

        public BoltQuerySession( Neo4jTransactionalContext transactionalContext, String querySource )
        {
            super( transactionalContext );
            this.querySource = querySource;
        }

        @Override
        public String toString()
        {
            return format( "bolt-session\t%s", querySource );
        }
    }
}
