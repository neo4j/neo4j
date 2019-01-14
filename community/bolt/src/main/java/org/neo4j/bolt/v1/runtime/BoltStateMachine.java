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
package org.neo4j.bolt.v1.runtime;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltConnectionDescriptor;
import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.kernel.api.security.AuthToken.PRINCIPAL;
import static org.neo4j.values.storable.Values.stringArray;

/**
 * This state machine oversees the exchange of messages for the Bolt protocol.
 * Central to this are the five active states -- CONNECTED, READY, STREAMING,
 * FAILED and INTERRUPTED -- as well as the transitions between them which
 * correspond to the Bolt protocol request messages INIT, ACK_FAILURE, RESET,
 * RUN, DISCARD_ALL and PULL_ALL. Of particular note is RESET which exhibits
 * dual behaviour in both marking the current query for termination and clearing
 * down the current connection state.
 * <p>
 * To help ensure a secure protocol, any transition not explicitly defined here
 * (i.e. a message sent out of sequence) will result in an immediate failure
 * response and a closed connection.
 */
public class BoltStateMachine implements AutoCloseable, ManagedBoltStateMachine
{
    private final String id;
    private final BoltChannel boltChannel;
    private final Clock clock;
    private final Log log;

    State state = State.CONNECTED;

    final SPI spi;
    final MutableConnectionState ctx;

    public BoltStateMachine( SPI spi, BoltChannel boltChannel, Clock clock, LogService logService )
    {
        this.id = boltChannel.id();
        this.spi = spi;
        this.ctx = new MutableConnectionState( spi, clock );
        this.boltChannel = boltChannel;
        this.clock = clock;
        this.log = logService.getInternalLog( getClass() );
    }

    public State state()
    {
        return state;
    }

    public StatementProcessor statementProcessor()
    {
        return ctx.statementProcessor;
    }

    private void before( BoltResponseHandler handler ) throws BoltConnectionFatality
    {
        if ( ctx.isTerminated.get() )
        {
            close();
        }
        else if ( ctx.interruptCounter.get() > 0 )
        {
            state = state.interrupt( this );
        }
        ctx.responseHandler = handler;
    }

    private void after()
    {
        if ( ctx.responseHandler != null )
        {
            try
            {
                Neo4jError pendingError = ctx.pendingError;
                if ( pendingError != null )
                {
                    ctx.markFailed( pendingError );
                }

                if ( ctx.pendingIgnore )
                {
                    ctx.markIgnored();
                }

                ctx.resetPendingFailedAndIgnored();
                ctx.responseHandler.onFinish();
            }
            finally
            {
                ctx.responseHandler = null;
            }
        }
    }

    /**
     * Initialize the session.
     */
    public void init( String userAgent, Map<String,Object> authToken,
            BoltResponseHandler handler ) throws BoltConnectionFatality
    {
        before( handler );
        try
        {
            if ( ctx.canProcessMessage() )
            {
                state = state.init( this, userAgent, authToken );
            }
        }
        finally
        {
            after();
        }
    }

    /**
     * Clear any outstanding error condition. This differs from {@link #reset(BoltResponseHandler)} in two
     * important ways:
     * <p>
     * 1) If there was an explicitly created transaction, the session will move back
     * to IN_TRANSACTION, rather than IDLE. This allows a more natural flow for client
     * side drivers, where explicitly opened transactions always are ended with COMMIT or ROLLBACK,
     * even if an error occurs. In all other cases, the session will move to the IDLE state.
     * <p>
     * 2) It will not interrupt any ahead-in-line messages.
     */
    public void ackFailure( BoltResponseHandler handler ) throws BoltConnectionFatality
    {
        before( handler );
        try
        {
            // it should always be fine to ACK_FAILURE thus no canProcessMessage check
            state = state.ackFailure( this );
        }
        finally
        {
            after();
        }
    }

    /**
     * Reset the session to an IDLE state. This clears any outstanding failure condition, disposes
     * of any outstanding result records and rolls back the current transaction (if any).
     * <p>
     * This differs from {@link #ackFailure(BoltResponseHandler)} in that it is more "radical" - it does not
     * matter what the state of the session is, as long as it is open, reset will move it back to IDLE.
     * <p>
     * This is designed to cater to two use cases:
     * <p>
     * 1) Rather than create new sessions over and over, drivers can maintain a pool of sessions,
     * and reset them before each re-use. Since establishing sessions can be high overhead,
     * this is quite helpful.
     * 2) Kill any stuck or long running operation
     */
    public void reset( BoltResponseHandler handler ) throws BoltConnectionFatality
    {
        before( handler );
        try
        {
            // it should always be fine to RESET thus no canProcessMessage check
            state = state.reset( this );
        }
        finally
        {
            after();
        }
    }

    /**
     * Run a statement, yielding a result stream which can be retrieved through pulling it in a subsequent call.
     * <p/>
     * If there is a statement running already, all remaining items in its stream must be
     * {@link #pullAll(BoltResponseHandler) pulled} or {@link #discardAll(BoltResponseHandler)
     * discarded}.
     */
    public void run( String statement, MapValue params, BoltResponseHandler handler )
            throws BoltConnectionFatality
    {
        long start = clock.millis();
        before( handler );
        try
        {
            if ( ctx.canProcessMessage() )
            {
                state = state.run( this, statement, params );
                handler.onMetadata( "result_available_after", Values.longValue( clock.millis() - start ) );
            }
        }
        finally
        {
            after();
        }
    }

    /**
     * Discard all the remaining entries in the current result stream. This has the same semantic behavior as
     * {@link #pullAll(BoltResponseHandler)}, but without actually retrieving the stream.
     * This is useful for completing the run of a statement when you don't care about the data result.
     */
    public void discardAll( BoltResponseHandler handler ) throws BoltConnectionFatality
    {
        before( handler );
        try
        {
            if ( ctx.canProcessMessage() )
            {
                state = state.discardAll( this );
            }
        }
        finally
        {
            after();
        }
    }

    /**
     * Retrieve all remaining entries in the current result. This is a distinct operation from 'run' in order to
     * enable pulling the output stream in chunks controlled by the user
     */
    public void pullAll( BoltResponseHandler handler ) throws BoltConnectionFatality
    {
        before( handler );
        try
        {
            if ( ctx.canProcessMessage() )
            {
                state = state.pullAll( this );
            }
        }
        finally
        {
            after();
        }
    }

    public void markFailed( Neo4jError error )
    {
        fail( this, error );
        state = State.FAILED;
    }

    /** A session id that is unique for this database instance */
    public String key()
    {
        return id;
    }

    /**
     * When this is invoked, the machine will make attempts
     * at interrupting any currently running action,
     * and will then ignore all inbound messages until a {@link #reset(BoltResponseHandler) reset}
     * message is received. If this is called multiple times, an equivalent number
     * of reset messages must be received before the SSM goes back to a good state.
     * <p>
     * You can imagine this is as a "call ahead" mechanism used by RESET to
     * cancel any statements ahead of it in line, without compromising the single-
     * threaded processing of messages that the state machine does.
     * <p>
     * This can be used to cancel a long-running statement or transaction.
     */
    public void interrupt()
    {
        ctx.interruptCounter.incrementAndGet();
        ctx.statementProcessor.markCurrentTransactionForTermination();
    }

    /**
     * When this is invoked, the machine will check whether the related transaction is
     * marked for termination and will reset the TransactionStateMachine to AUTO_COMMIT mode
     * while releasing the related transactional resources.
     */
    public void validateTransaction() throws KernelException
    {
        ctx.statementProcessor.validateTransaction();
    }

    public void externalError( Neo4jError error, BoltResponseHandler handler ) throws BoltConnectionFatality
    {
        before( handler );
        try
        {
            if ( ctx.canProcessMessage() )
            {
                fail( this, error );
                this.state = State.FAILED;
            }
        }
        finally
        {
            after();
        }
    }

    public boolean isClosed()
    {
        return ctx.closed;
    }

    @Override
    public void close()
    {
        try
        {
            boltChannel.close();
        }
        finally
        {
            spi.onTerminate( this );
            ctx.closed = true;
            //However a new transaction may have been created
            //so we must always to reset
            reset();
        }
    }

    @Override
    public String owner()
    {
        return ctx.owner;
    }

    @Override
    public void terminate()
    {
        /*
         * This is a side-channel call and we should not close anything directly.
         * Just mark the transaction and set isTerminated to true and then the session
         * thread will close down the connection eventually.
            */
        ctx.isTerminated.set( true );
        ctx.statementProcessor.markCurrentTransactionForTermination();
        spi.onTerminate( this );
    }

    @Override
    public boolean willTerminate()
    {
        return ctx.isTerminated.get();
    }

    public boolean shouldStickOnThread()
    {
        // Currently, we're doing our best to keep things together
        // We should not switch threads when there's an active statement (executing/streaming)
        // Also, we're currently sticking to the thread when there's an open transaction due to
        // cursor errors we receive when a transaction is picked up by another thread linearly.
        return statementProcessor().hasTransaction() || statementProcessor().hasOpenStatement();
    }

    public boolean hasOpenStatement()
    {
        return statementProcessor().hasOpenStatement();
    }

    public enum State
    {
        /**
         * Following the socket connection and a small handshake exchange to
         * establish protocol version, the machine begins in the CONNECTED
         * state. The <em>only</em> valid transition from here is through a
         * correctly authorised INIT into the READY state. Any other action
         * results in disconnection.
         */
        CONNECTED
                {
                    @Override
                    public State init( BoltStateMachine machine, String userAgent,
                            Map<String,Object> authToken ) throws BoltConnectionFatality
                    {
                        try
                        {
                            AuthenticationResult authResult = machine.spi.authenticate( authToken );
                            machine.ctx.init( authResult );
                            if ( authResult.credentialsExpired() )
                            {
                                machine.ctx.onMetadata( "credentials_expired", Values.TRUE );
                            }
                            machine.ctx.onMetadata( "server", Values.stringValue( machine.spi.version() ) );

                            machine.spi.udcRegisterClient( userAgent );
                            if ( authToken.containsKey( PRINCIPAL ) )
                            {
                                machine.ctx.owner = authToken.get( PRINCIPAL ).toString();
                            }
                            machine.ctx.setQuerySourceFromClientNameAndPrincipal(
                                    userAgent, machine.ctx.owner, machine.spi.connectionDescriptor() );
                            if ( machine.ctx.owner != null )
                            {
                                machine.spi.register( machine, machine.ctx.owner );
                            }
                            return READY;
                        }
                        catch ( Throwable t )
                        {
                            return handleFailure( machine, t, true );
                        }
                    }
                },

        /**
         * The READY state indicates that the connection is ready to accept a
         * new RUN request. This is the "normal" state for a connection and
         * becomes available after successful authorisation and when not
         * executing another statement. It is this that ensures that statements
         * must be executed in series and each must wait for the previous
         * statement to complete.
         */
        READY
                {
                    @Override
                    public State run( BoltStateMachine machine, String statement,
                            MapValue params ) throws BoltConnectionFatality
                    {
                        try
                        {
                            StatementMetadata statementMetadata =
                                    machine.ctx.statementProcessor.run( statement, params );
                            machine.ctx.onMetadata( "fields", stringArray( statementMetadata.fieldNames() ) );
                            return STREAMING;
                        }
                        catch ( AuthorizationExpiredException e )
                        {
                            return handleFailure( machine, e, true );
                        }
                        catch ( Throwable t )
                        {
                            return handleFailure( machine, t );
                        }
                    }

                    @Override
                    public State interrupt( BoltStateMachine machine )
                    {
                        return INTERRUPTED;
                    }

                    @Override
                    public State reset( BoltStateMachine machine ) throws BoltConnectionFatality
                    {
                        return resetMachine( machine );
                    }
                },

        /**
         * When STREAMING, a result is available as a stream of records.
         * These must be PULLed or DISCARDed before any further statements
         * can be executed.
         */
        STREAMING
                {
                    @Override
                    public State interrupt( BoltStateMachine machine )
                    {
                        return INTERRUPTED;
                    }

                    @Override
                    public State reset( BoltStateMachine machine ) throws BoltConnectionFatality
                    {
                        return resetMachine( machine );
                    }

                    @Override
                    public State pullAll( BoltStateMachine machine ) throws BoltConnectionFatality
                    {
                        try
                        {
                            machine.ctx.statementProcessor.streamResult( recordStream ->
                                    machine.ctx.responseHandler.onRecords( recordStream, true ) );

                            return READY;
                        }
                        catch ( AuthorizationExpiredException e )
                        {
                            return handleFailure( machine, e, true );
                        }
                        catch ( Throwable e )
                        {
                            return handleFailure( machine, e );
                        }
                    }

                    @Override
                    public State discardAll( BoltStateMachine machine ) throws BoltConnectionFatality
                    {
                        try
                        {
                            machine.ctx.statementProcessor.streamResult( recordStream ->
                                    machine.ctx.responseHandler.onRecords( recordStream, false ) );

                            return READY;
                        }
                        catch ( AuthorizationExpiredException e )
                        {
                            return handleFailure( machine, e, true );
                        }
                        catch ( Throwable t )
                        {
                            return handleFailure( machine, t );
                        }
                    }
                },

        /**
         * The FAILED state occurs when a recoverable error is encountered.
         * This might be something like a Cypher SyntaxError or
         * ConstraintViolation. To exit the FAILED state, either a RESET
         * or and ACK_FAILURE must be issued. All stream will be IGNORED
         * until this is done.
         */
        FAILED
                {
                    @Override
                    public State interrupt( BoltStateMachine machine )
                    {
                        return INTERRUPTED;
                    }

                    @Override
                    public State reset( BoltStateMachine machine ) throws BoltConnectionFatality
                    {
                        return resetMachine( machine );
                    }

                    @Override
                    public State ackFailure( BoltStateMachine machine )
                    {
                        machine.ctx.resetPendingFailedAndIgnored();
                        return READY;
                    }

                    @Override
                    public State run( BoltStateMachine machine, String statement,
                            MapValue params )
                    {
                        machine.ctx.markIgnored();
                        return FAILED;
                    }

                    @Override
                    public State pullAll( BoltStateMachine machine )
                    {
                        machine.ctx.markIgnored();
                        return FAILED;
                    }

                    @Override
                    public State discardAll( BoltStateMachine machine )
                    {
                        machine.ctx.markIgnored();
                        return FAILED;
                    }
                },

        /**
         * If the state machine has been INTERRUPTED then a RESET message
         * has entered the queue and is waiting to be processed. The initial
         * interrupt forces the current statement to stop and all subsequent
         * requests to be IGNORED until the RESET itself is processed.
         */
        INTERRUPTED
                {
                    @Override
                    public State interrupt( BoltStateMachine machine )
                    {
                        return INTERRUPTED;
                    }

                    @Override
                    public State reset( BoltStateMachine machine ) throws BoltConnectionFatality
                    {
                        if ( machine.ctx.interruptCounter.decrementAndGet() > 0 )
                        {
                            machine.ctx.markIgnored();
                            return INTERRUPTED;
                        }
                        return resetMachine( machine );
                    }

                    @Override
                    public State ackFailure( BoltStateMachine machine )
                    {
                        machine.ctx.markIgnored();
                        return INTERRUPTED;
                    }

                    @Override
                    public State run( BoltStateMachine machine, String statement, MapValue params )
                    {
                        machine.ctx.markIgnored();
                        return INTERRUPTED;
                    }

                    @Override
                    public State pullAll( BoltStateMachine machine )
                    {
                        machine.ctx.markIgnored();
                        return INTERRUPTED;
                    }

                    @Override
                    public State discardAll( BoltStateMachine machine )
                    {
                        machine.ctx.markIgnored();
                        return INTERRUPTED;
                    }
                };

        public State init( BoltStateMachine machine, String userAgent, Map<String,Object> authToken )
                throws BoltConnectionFatality
        {
            String msg = "INIT cannot be handled by a session in the " + name() + " state.";
            fail( machine, Neo4jError.fatalFrom( Status.Request.Invalid, msg ) );
            throw new BoltProtocolBreachFatality( msg );
        }

        public State ackFailure( BoltStateMachine machine ) throws BoltConnectionFatality
        {
            String msg = "ACK_FAILURE cannot be handled by a session in the " + name() + " state.";
            fail( machine, Neo4jError.fatalFrom( Status.Request.Invalid, msg ) );
            throw new BoltProtocolBreachFatality( msg );
        }

        public State interrupt( BoltStateMachine machine ) throws BoltConnectionFatality
        {
            // The message below is correct, not a copy-paste error. Interrupts are triggered by
            // a RESET message.
            String msg = "RESET cannot be handled by a session in the " + name() + " state.";
            fail( machine, Neo4jError.fatalFrom( Status.Request.Invalid, msg ) );
            throw new BoltProtocolBreachFatality( msg );
        }

        public State reset( BoltStateMachine machine ) throws BoltConnectionFatality
        {
            String msg = "RESET cannot be handled by a session in the " + name() + " state.";
            fail( machine, Neo4jError.fatalFrom( Status.Request.Invalid, msg ) );
            throw new BoltProtocolBreachFatality( msg );
        }

        public State run( BoltStateMachine machine, String statement, MapValue params ) throws
                BoltConnectionFatality
        {
            String msg = "RUN cannot be handled by a session in the " + name() + " state.";
            fail( machine, Neo4jError.fatalFrom( Status.Request.Invalid, msg ) );
            throw new BoltProtocolBreachFatality( msg );
        }

        public State discardAll( BoltStateMachine machine ) throws BoltConnectionFatality
        {
            String msg = "DISCARD_ALL cannot be handled by a session in the " + name() + " state.";
            fail( machine, Neo4jError.fatalFrom( Status.Request.Invalid, msg ) );
            throw new BoltProtocolBreachFatality( msg );
        }

        public State pullAll( BoltStateMachine machine ) throws BoltConnectionFatality
        {
            String msg = "PULL_ALL cannot be handled by a session in the " + name() + " state.";
            fail( machine, Neo4jError.fatalFrom( Status.Request.Invalid, msg ) );
            throw new BoltProtocolBreachFatality( msg );
        }

        State resetMachine( BoltStateMachine machine ) throws BoltConnectionFatality
        {
            try
            {
                machine.ctx.resetPendingFailedAndIgnored();
                machine.ctx.statementProcessor.reset();
                return READY;
            }
            catch ( Throwable t )
            {
                return handleFailure( machine, t, true );
            }
        }
    }

    private static State handleFailure( BoltStateMachine machine, Throwable t ) throws BoltConnectionFatality
    {
        return handleFailure( machine, t, false );
    }

    private static State handleFailure( BoltStateMachine machine, Throwable t, boolean fatal ) throws BoltConnectionFatality
    {
        if ( ExceptionUtils.indexOfType( t, BoltConnectionFatality.class ) != -1 )
        {
            fatal = true;
        }

        return handleFailure( machine, t, fatal ? Neo4jError.fatalFrom( t ) : Neo4jError.from( t ) );
    }

    private static State handleFailure( BoltStateMachine machine, Throwable t, Neo4jError error ) throws BoltConnectionFatality
    {
        fail( machine, error );

        if ( error.isFatal() )
        {
            if ( ExceptionUtils.indexOfType( t, AuthorizationExpiredException.class ) != -1 )
            {
                throw new BoltConnectionAuthFatality( "Failed to process a bolt message", t );
            }
            if ( t instanceof AuthenticationException )
            {
                throw new BoltConnectionAuthFatality( (AuthenticationException) t );
            }

            throw new BoltConnectionFatality( "Failed to process a bolt message", t );
        }

        return State.FAILED;
    }

    private static void fail( BoltStateMachine machine, Neo4jError neo4jError )
    {
        machine.spi.reportError( neo4jError );
        if ( machine.state == State.FAILED )
        {
            machine.ctx.markIgnored();
        }
        else
        {
            machine.ctx.markFailed( neo4jError );
        }
    }

    private void reset()
    {
        try
        {
            ctx.statementProcessor.reset();
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    static class MutableConnectionState implements BoltResponseHandler
    {
        private static final NullStatementProcessor NULL_STATEMENT_PROCESSOR = new NullStatementProcessor();
        private final SPI spi;
        private final Clock clock;

        /**
         * Callback poised to receive the next response
         */
        BoltResponseHandler responseHandler;

        Neo4jError pendingError;

        boolean pendingIgnore;

        /**
         * This is incremented each time {@link #interrupt()} is called,
         * and decremented each time a {@link BoltStateMachine#reset(BoltResponseHandler)} message
         * arrives. When this is above 0, all messages will be ignored.
         * This way, when a reset message arrives on the network, interrupt
         * can be called to "purge" all the messages ahead of the reset message.
         */
        final AtomicInteger interruptCounter = new AtomicInteger();

        final AtomicBoolean isTerminated = new AtomicBoolean( false );

        StatementProcessor statementProcessor = NULL_STATEMENT_PROCESSOR;

        String owner;

        boolean closed;

        MutableConnectionState( SPI spi, Clock clock )
        {
            this.spi = spi;
            this.clock = clock;
        }

        private void init( AuthenticationResult authenticationResult )
        {
            this.statementProcessor = new TransactionStateMachine( spi.transactionSpi(), authenticationResult, clock );
        }

        private void setQuerySourceFromClientNameAndPrincipal( String clientName, String principal,
                BoltConnectionDescriptor connectionDescriptor )
        {
            String principalName = principal == null ? "null" : principal;
            statementProcessor.setQuerySource( new BoltQuerySource( principalName, clientName, connectionDescriptor ) );
        }

        @Override
        public void onStart()
        {
            if ( responseHandler != null )
            {
                responseHandler.onStart();
            }
        }

        @Override
        public void onRecords( BoltResult result, boolean pull ) throws Exception
        {
            if ( responseHandler != null )
            {
                responseHandler.onRecords( result, pull );
            }
        }

        @Override
        public void onMetadata( String key, AnyValue value )
        {
            if ( responseHandler != null )
            {
                responseHandler.onMetadata( key, value );
            }
        }

        @Override
        public void markIgnored()
        {
            if ( responseHandler != null )
            {
                responseHandler.markIgnored();
            }
            else
            {
                pendingIgnore = true;
            }
        }

        @Override
        public void markFailed( Neo4jError error )
        {
            if ( responseHandler != null )
            {
                responseHandler.markFailed( error );
            }
            else
            {
                pendingError = error;
            }
        }

        @Override
        public void onFinish()
        {
            if ( responseHandler != null )
            {
                responseHandler.onFinish();
            }
        }

        private boolean canProcessMessage()
        {
            return !closed && pendingError == null && !pendingIgnore;
        }

        private void resetPendingFailedAndIgnored()
        {
            pendingError = null;
            pendingIgnore = false;
        }

    }

    public interface SPI
    {
        void reportError( Neo4jError err );

        AuthenticationResult authenticate( Map<String,Object> authToken ) throws AuthenticationException;

        void udcRegisterClient( String clientName );

        BoltConnectionDescriptor connectionDescriptor();

        void register( BoltStateMachine machine, String owner );

        TransactionStateMachine.SPI transactionSpi();

        void onTerminate( BoltStateMachine machine );

        String version();
    }

    private static class NullStatementProcessor implements StatementProcessor
    {
        @Override
        public StatementMetadata run( String statement, MapValue params )
        {
            throw new UnsupportedOperationException( "Unable to run any statements." );
        }

        @Override
        public void streamResult( ThrowingConsumer<BoltResult,Exception> resultConsumer )
        {
            throw new UnsupportedOperationException( "Unable to stream any results." );
        }

        @Override
        public void reset()
        {
            // nothing to reset
        }

        @Override
        public void markCurrentTransactionForTermination()
        {
            // nothing to mark
        }

        @Override
        public void validateTransaction()
        {
            // nothing to validate
        }

        @Override
        public boolean hasTransaction()
        {
            return false;
        }

        @Override
        public boolean hasOpenStatement()
        {
            return false;
        }

        @Override
        public void setQuerySource( BoltQuerySource querySource )
        {
            // nothing to do
        }
    }

}
