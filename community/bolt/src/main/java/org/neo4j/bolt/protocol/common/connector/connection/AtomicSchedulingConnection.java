/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.connector.connection;

import io.netty.channel.Channel;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.bolt.BoltServer;
import org.neo4j.bolt.fsm.StateMachine;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connection.Job;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.protocol.common.fsm.error.AuthenticationStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.response.FailureMessage;
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.bolt.protocol.error.BoltNetworkException;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.dbms.admissioncontrol.AdmissionControlService;
import org.neo4j.dbms.admissioncontrol.AdmissionControlToken;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.NotificationConfiguration;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.FeatureToggles;

/**
 * Provides a non-blocking connection implementation.
 * <p />
 * This implementation makes heavy use of atomics in order to ensure consistent execution of request and shutdown tasks
 * throughout the connection lifetime.
 */
public class AtomicSchedulingConnection extends AbstractConnection {

    private static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(AtomicSchedulingConnection.class);

    private static final int BATCH_SIZE = FeatureToggles.getInteger(BoltServer.class, "max_batch_size", 100);

    private final ExecutorService executor;
    private final Clock clock;

    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);
    private volatile Thread workerThread;
    private final LinkedBlockingDeque<Job> jobs = new LinkedBlockingDeque<>();

    private final AtomicInteger remainingInterrupts = new AtomicInteger();
    private final AtomicReference<Transaction> transaction = new AtomicReference<>();

    /**
     * A flag to denote that the connection should create a new admission control token.
     * This is set to false when a transaction receives it's first RUN currently, and is reset when the transaction
     * finishes. This is likely to change, but as a solution it works.
     */
    private final AtomicBoolean connectionRequiresAdmissionControl = new AtomicBoolean(true);

    public AtomicSchedulingConnection(
            Connector connector,
            String id,
            Channel channel,
            long connectedAt,
            MemoryTracker memoryTracker,
            LogService logService,
            ExecutorService executor,
            Clock clock,
            AdmissionControlService admissionControlService) {
        super(connector, id, channel, connectedAt, memoryTracker, logService, admissionControlService);
        this.executor = executor;
        this.clock = clock;
    }

    @Override
    public boolean isIdling() {
        return this.state.get() == State.IDLE && !this.hasPendingJobs();
    }

    @Override
    public boolean hasPendingJobs() {
        return !this.jobs.isEmpty();
    }

    @Override
    public void submit(RequestMessage message) {
        this.notifyListeners(listener -> listener.onRequestReceived(message));
        if (this.admissionControl.enabled()
                && message.requiresAdmissionControl()
                && this.connectionRequiresAdmissionControl.compareAndSet(true, false)) {
            this.submit(new ProcessJob(this, this.clock.millis(), message, this.admissionControl.requestToken()));
        } else {
            this.submit(new ProcessJob(this, this.clock.millis(), message, null));
        }
    }

    @Override
    public void submit(Job job) {
        this.jobs.addLast(job);
        this.schedule(true);
    }

    /**
     * Attempts to schedule a connection for job execution.
     * <p />
     * This function will effectively act as a NOOP when this connection has already been scheduled for execution or has
     * no remaining jobs to execute.
     *
     * @param submissionHint true if job submission has taken place just prior to invocation, false otherwise.
     */
    private void schedule(boolean submissionHint) {
        // ensure that the caller either explicitly indicates that they submitted a job or a job has been queued within
        // the connection internal queue - this is necessary in order to solve a race condition in which jobs may be
        // lost when the current executor finishes up while a new job is submitted
        if (!submissionHint && !this.hasPendingJobs()) {
            return;
        }

        // assuming scheduling is permitted (e.g. has not yet occurred in another thread and the connection remains
        // alive), we'll actually schedule another batch through our executor service
        if (this.state.compareAndSet(State.IDLE, State.SCHEDULED)) {
            log.debug("[%s] Scheduling connection for execution", this.id);
            this.notifyListeners(ConnectionListener::onScheduled);

            try {
                this.executor.submit(this::executeJobs);
            } catch (RejectedExecutionException ex) {
                // we get RejectedExecutionException when all threads within the pool are busy (e.g. the server is at
                // capacity) and the queue (if any) is at its limit - as a result, we immediately return FAILURE and
                // terminate the connection to free up resources
                var error = Error.from(
                        Status.Request.NoThreadsAvailable,
                        Status.Request.NoThreadsAvailable.code().description());

                this.connector().errorAccountant().notifyThreadStarvation(this, ex);
                this.notifyListenersSafely("requestResultFailure", listener -> listener.onResponseFailed(error));

                this.channel.writeAndFlush(new FailureMessage(error.status(), error.message(), false));
                this.close();
            }
        }
    }

    @Override
    public boolean inWorkerThread() {
        var workerThread = this.workerThread;
        var currentThread = Thread.currentThread();

        return workerThread == currentThread;
    }

    /**
     * Executes the remaining jobs within this connection.
     */
    private void executeJobs() {
        var currentThread = Thread.currentThread();

        // adjust the thread name to ensure that the connection is being identified correctly within the application
        // logs
        // TODO: Should be handled in a neater manner - MDC?
        var originalThreadName = currentThread.getName();
        var customizedThreadName =
                String.format("%s [%s - %s]", originalThreadName, this.id, this.channel.remoteAddress());

        currentThread.setName(customizedThreadName);

        log.debug("[%s] Activating connection", this.id);

        // claim ownership of the current thread
        this.workerThread = currentThread;

        var startTime = this.clock.millis();
        this.notifyListeners(ConnectionListener::onActivated);
        try {
            this.doExecuteJobs();
        } catch (Throwable ex) {
            log.error("[" + this.id + "] Uncaught exception during job execution", ex);
            this.close();
        } finally {
            var timeBound = this.clock.millis() - startTime;
            this.notifyListeners((l) -> l.onIdle(timeBound));
            log.debug("[%s] Returning to idle state", this.id);

            // remove ownership of the current thread in order to ensure that future isOnWorkerThread calls no longer
            // succeed
            this.workerThread = null;

            // return the thread name back to its original value
            currentThread.setName(originalThreadName);

            // revert scheduled flag to its original state and attempt to schedule the thread again in case that new
            // jobs have been submitted while this thread was finishing up (this prevents race conditions in which jobs
            // may be "lost")
            var previousState = this.state.compareAndExchange(State.SCHEDULED, State.IDLE);
            switch (previousState) {
                case SCHEDULED -> this.schedule(false);

                case CLOSING ->
                // if we did not successfully return the connection to its idle state, and it has been marked for
                // termination, we'll make sure to terminate it now as the original caller did not complete this
                // step during our execution phase
                this.doClose();

                case CLOSED ->
                // if the connection has been closed during this execution cycle, we'll simply log this fact for
                // debugging purposes - there is nothing else to do here as this object is effectively considered dead
                // at this point and has already been removed from the connection registry
                log.debug("[%s] Connection has already been terminated via its worker thread", this.id);
            }
        }
    }

    /**
     * Executes the remaining jobs within this connection.
     */
    private void doExecuteJobs() {
        var fsm = this.fsm();
        var batch = new ArrayList<Job>(BATCH_SIZE);

        // poll for new jobs until the connection is closed (either through a client side disconnect or a server-side
        // error/operator intervention)
        while (this.isActive()) {
            // we'll prioritize batched execution as this provides a slight performance advantage over regularly polling
            // due to the reduced lock contention on the underlying job queue
            this.jobs.drainTo(batch, BATCH_SIZE);

            if (!batch.isEmpty()) {
                log.debug("[%s] Executing %d scheduled jobs", this.id, batch.size());

                // keep iterating through the queue so long as the connection has not been marked for closure or closed
                // from this thread as a result of an error or termination command
                var it = batch.iterator();
                while (it.hasNext() && this.isActive()) {
                    this.executeJob(fsm, it.next());
                }

            } else {
                // if there are no jobs, we'll terminate unless there are open transactions or statements remaining
                // which require us to remain on this thread
                if (this.transaction().isEmpty()
                        && this.connector().configuration().enableTransactionThreadBinding()) {
                    break;
                }

                // since we're unable to retrieve jobs at the moment, we'll switch to single-job polling for the next
                // iteration as the queue will notify us as soon as a new job is queued (or the timeout is exceeded)
                Job job = null;
                try {
                    log.debug("[%s] Waiting for additional jobs", this.id);

                    var timeoutMillis = this.connector()
                            .configuration()
                            .threadBindingTimeout()
                            .toMillis();
                    if (this.connector().configuration().enableTransactionThreadBinding()) {
                        // FIXME: Still relies on the legacy hardcoded timeout as we do not want to
                        //        alter this behavior quite yet. Update this block when ready.
                        job = this.jobs.pollFirst(10, TimeUnit.SECONDS);
                    } else if (timeoutMillis != 0) {
                        job = this.jobs.pollFirst(timeoutMillis, TimeUnit.MILLISECONDS);
                    }
                } catch (InterruptedException ex) {
                    // this condition may occur in two different cases:
                    //
                    //  #1 - Soft Shutdown
                    //       During soft shutdown, the executor service will be asked to terminate any free threads
                    //       within the pool and interrupt any remaining waiting tasks - It will, however, not mark
                    //       connections with open transactions for termination
                    //
                    //  #2 - Hard Shutdown
                    //       Once a configurable grace period has been exceeded, the executor service will mark all
                    //       connections for closure and force-close any non-compliant threads
                    //
                    // the outcome of this condition is identified by the CLOSING state and will thus be evaluated
                    // in the next cycle of this loop
                    log.debug("[" + this.id + "] Worker interrupted while awaiting new jobs", ex);
                }

                if (job != null) {
                    this.executeJob(fsm, job);
                } else {
                    // If no job was found in timeout period, check the fsm is still valid,
                    if (!fsm.validate() || !this.connector().configuration().enableTransactionThreadBinding()) {
                        // if fsm is not valid then the executing worker thread can be released.
                        break;
                    }
                }
            }

            // make sure that we clear out any previously executed jobs from the batch list as these would otherwise
            // execute again within the next cycle - this is necessary as we avoid unnecessary allocations by keeping
            // a sized ArrayList around so long as the connection remains active
            batch.clear();
        }
    }

    private void executeJob(StateMachine fsm, Job job) {
        this.channel.write(StateSignal.BEGIN_JOB_PROCESSING);

        try {
            job.perform(fsm, this.responseHandler);
        } catch (AuthenticationStateTransitionException ex) {
            this.close();

            if (!(ex.getCause() instanceof AuthorizationExpiredException)) {
                userLog.warn("[" + this.id + "] " + ex.getMessage());
            }
        } catch (StateMachineException ex) {
            // when a state machine exception bubbles up to here, it is non-status bearing or has
            // occurred when invoking a state machine method outside the scope of a request thus
            // warranting connection termination
            this.close();

            log.warn("[" + this.id + "] Terminating connection due to state machine error", ex);
        } catch (Throwable ex) {
            if (ex instanceof BoltNetworkException) {
                log.debug("[" + this.id + "] Terminating connection due to network error", ex);
                this.connector().errorAccountant().notifyNetworkAbort(this, ex);
            } else {
                userLog.error("[" + this.id + "] Terminating connection due to unexpected error", ex);
            }

            this.close();
        } finally {
            this.channel.write(StateSignal.END_JOB_PROCESSING);
        }
    }

    @Override
    public boolean isInterrupted() {
        return this.remainingInterrupts.get() != 0;
    }

    @Override
    public Transaction beginTransaction(
            TransactionType type,
            String databaseName,
            AccessMode mode,
            List<String> bookmarks,
            Duration timeout,
            Map<String, Object> metadata,
            NotificationsConfig transactionNotificationsConfig)
            throws TransactionException {
        // if no database name was given explicitly, we'll substitute it with the current default
        // database on this connection (e.g. either a pre-selected database, the user home database
        // or the system-wide home database)
        if (databaseName == null) {
            databaseName = this.selectedDefaultDatabase();
        }

        var notificationsConfig = resolveNotificationsConfig(transactionNotificationsConfig);

        // optimistically create the transaction as we do not know what state the connection is in
        // at the moment
        var transaction = this.connector()
                .transactionManager()
                .create(type, this, databaseName, mode, bookmarks, timeout, metadata, notificationsConfig);
        // if another transaction has been created in the meantime or was already present when the
        // method was originally invoked, we'll destroy the optimistically created transaction and
        // throw immediately to indicate misuse
        if (!this.transaction.compareAndSet(null, transaction)) {
            try {
                transaction.close();
            } catch (TransactionException ignore) {
            }

            throw new IllegalStateException("Nested transactions are not supported");
        }

        return transaction;
    }

    private NotificationConfiguration resolveNotificationsConfig(NotificationsConfig txConfig) {
        if (txConfig != null) {
            return txConfig.buildConfiguration(this.notificationsConfig);
        }

        if (this.notificationsConfig != null) {
            this.notificationsConfig.buildConfiguration(null);
        }

        return null;
    }

    @Override
    public Optional<Transaction> transaction() {
        return Optional.ofNullable(this.transaction.get());
    }

    @Override
    public void closeTransaction() throws TransactionException {
        this.connectionRequiresAdmissionControl.set(true);

        var tx = this.transaction.getAndSet(null);
        if (tx == null) {
            return;
        }

        tx.close();
    }

    @Override
    public void interrupt() {
        // increment the interrupt timer internally in order to keep track on when we are supposed
        // to reset to a valid state
        var previous = this.remainingInterrupts.getAndIncrement();

        // interrupt the state machine and cancel any active transactions only when this is the
        // first interrupt
        if (previous == 0) {
            // notify the state machine so that it no longer processes any further messages until
            // explicitly reset
            this.fsm.interrupt();

            // if there is currently an active transaction, we'll interrupt it and all of its children
            // in order to free up the worker threads immediately
            var tx = this.transaction.get();
            if (tx != null && !tx.hasFailed()) {
                tx.interrupt();
            }
        }

        // schedule a task with the FSM so that the connection is reset correctly once all prior
        // messages have been handled
        this.submit((fsm, responseHandler) -> {
            if (this.reset()) {
                fsm.reset();
                responseHandler.onSuccess();
            } else {
                responseHandler.onIgnored();
            }
        });
    }

    @Override
    public boolean reset() {
        // this implementation roughly matches the JDK implementation of decrementAndGet with some additional sanity
        // checks to ensure that we don't go negative in case something goes horribly wrong
        int current;
        do {
            current = this.remainingInterrupts.get();

            // if the interrupt counter has already reached zero, there's nothing left for us to do - the connection is
            // available for further requests and operates normally (this can sometimes occur when drivers eagerly reset
            // as a result of their connection liveliness checks)
            if (current == 0) {
                return true;
            }
        } while (!this.remainingInterrupts.compareAndSet(current, current - 1));

        // if the loop doesn't complete immediately, we'll check whether the counter was previously at one meaning that
        // we have successfully reset the connection to the desired state
        if (current == 1) {
            try {
                this.closeTransaction();
            } catch (TransactionException ex) {
                log.warn("Failed to gracefully terminate transaction during reset", ex);
            }

            this.clearImpersonation();

            log.debug("[%s] Connection has been reset", this.id);
            return true;
        }

        log.debug("[%s] Interrupt has been cleared (%d interrupts remain active)", this.id, current - 1);
        return false;
    }

    @Override
    public boolean isActive() {
        var state = this.state.get();
        return state != State.CLOSING && state != State.CLOSED;
    }

    @Override
    public boolean isClosing() {
        return this.state.get() == State.CLOSING;
    }

    @Override
    public boolean isClosed() {
        return this.state.get() == State.CLOSED;
    }

    @Override
    public void close() {
        var inWorkerThread = this.inWorkerThread();

        State originalState;
        do {
            originalState = this.state.get();

            // ignore the call entirely if the current state is already CLOSING or CLOSED as another thread is likely
            // taking care of the cleanup procedure right now
            if ((!inWorkerThread && originalState == State.CLOSING) || originalState == State.CLOSED) {
                return;
            }
        } while (!this.state.compareAndSet(originalState, State.CLOSING));

        log.debug("[%s] Marked connection for closure", this.id);
        this.notifyListenersSafely("markForClosure", ConnectionListener::onMarkedForClosure);

        // if the connection was in idle when the closure occurred or if we're already on the worker thread, we'll
        // close the connection synchronously immediately in order to reduce congestion on the worker thread pool
        if (inWorkerThread || originalState == State.IDLE) {
            if (inWorkerThread) {
                log.debug("[%s] Close request from worker thread - Performing inline closure", this.id);
            } else {
                log.debug("[%s] Connection is idling - Performing inline closure", this.id);
            }

            this.doClose();
        } else {
            // interrupt any remaining workloads to ensure that the connection closes as fast as possible
            this.interrupt();

            // submit a noop job to wake up a worker thread waiting in single-job polling mode and have it realize that
            // the transaction is terminated
            submit((fsm, handler) -> {});
        }
    }

    /**
     * Performs the actual termination of this connection and its associated resources.
     * <p />
     * This function is invoked either through {@link #close()} when the connection has not been scheduled for execution
     * or through {@link #executeJobs()} when execution is still pending.
     */
    private void doClose() {
        // ensure that we are the first do transition the connection from closing to closed in order to prevent race
        // conditions between worker and network threads
        //
        // this is necessary as network threads as well as shutdown threads may take a connection to closed immediately
        // in some cases where there would otherwise be no guarantee that a worker will be scheduled.
        if (!this.state.compareAndSet(State.CLOSING, State.CLOSED)) {
            return;
        }

        log.debug("[%s] Closing connection", this.id);

        // attempt to cleanly terminate any pending transaction - this can sometimes fail as a
        // result of a prior error in which case we'll simply ignore the problem
        try {
            var transaction = this.transaction.getAndSet(null);
            if (transaction != null) {
                transaction.close();
            }
        } catch (TransactionException ex) {
            log.warn("[" + this.id + "] Failed to terminate transaction", ex);
        }

        BoltProtocol protocol;
        do {
            protocol = this.protocol.get();
        } while (!this.protocol.compareAndSet(protocol, null));

        // ensure that the underlying connection is also closed (the peer has likely already been notified of the
        // reason)
        this.channel.close().addListener(f -> {
            // also ensure that the associated memory tracker is closed as all associated resources will be destroyed as
            // soon as the connection is removed from its registry
            this.memoryTracker.close();
        });

        // notify any dependent components that the connection has completed its shutdown procedure and is now safe to
        // remove
        boolean isNegotiatedConnection = this.fsm != null;
        this.notifyListenersSafely(
                "close", connectionListener -> connectionListener.onConnectionClosed(isNegotiatedConnection));

        this.closeFuture.complete(null);
    }

    @Override
    public Future<?> closeFuture() {
        return this.closeFuture;
    }

    private enum State {
        IDLE,
        SCHEDULED,
        CLOSING,
        CLOSED
    }

    private record ProcessJob(
            AtomicSchedulingConnection conn, long queuedAt, RequestMessage message, AdmissionControlToken token)
            implements Job {
        @Override
        public void perform(StateMachine machine, ResponseHandler responseHandler) throws StateMachineException {
            var processingStartedAt = this.conn.clock.millis();
            var queuedForMillis = processingStartedAt - queuedAt;
            conn.notifyListeners(listener -> listener.onRequestBeginProcessing(message, queuedForMillis));
            try {
                conn.log.debug("[%s] Beginning execution of %s (queued for %d ms)", conn.id, message, queuedForMillis);
                conn.fsm.process(message, responseHandler, token);
            } catch (StateMachineException ex) {
                conn.notifyListeners(listener -> listener.onRequestFailedProcessing(message, ex));
                // re-throw the exception to let the scheduler handle the connection closure (if applicable)
                throw ex;
            } finally {
                var processedForMillis = this.conn.clock.millis() - processingStartedAt;
                conn.notifyListeners(listener -> listener.onRequestCompletedProcessing(message, processedForMillis));
                conn.log.debug("[%s] Completed execution of %s (took %d ms)", conn.id, message, processedForMillis);
            }
        }
    }

    public static class Factory implements Connection.Factory {
        private final ExecutorService executor;
        private final Clock clock;
        private final LogService logService;
        private final AdmissionControlService admissionControl;

        public Factory(
                ExecutorService executor,
                Clock clock,
                LogService logService,
                AdmissionControlService admissionControl) {
            this.executor = executor;
            this.clock = clock;
            this.logService = logService;
            this.admissionControl = admissionControl;
        }

        @Override
        public AtomicSchedulingConnection create(Connector connector, String id, Channel channel) {
            // TODO: Configurable chunk size for tuning?
            var memoryTracker = ConnectionMemoryTracker.createForPool(connector.memoryPool());
            memoryTracker.allocateHeap(SHALLOW_SIZE);

            return new AtomicSchedulingConnection(
                    connector,
                    id,
                    channel,
                    System.currentTimeMillis(),
                    memoryTracker,
                    this.logService,
                    this.executor,
                    this.clock,
                    this.admissionControl);
        }
    }
}
