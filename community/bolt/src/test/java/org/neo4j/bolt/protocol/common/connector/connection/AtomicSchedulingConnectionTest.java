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
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.neo4j.bolt.fsm.StateMachine;
import org.neo4j.bolt.fsm.StateMachineConfiguration;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connection.Job;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.connection.authentication.AuthenticationFlag;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.protocol.error.streaming.BoltStreamingWriteException;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.security.AuthenticationResult;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.testing.assertions.ClientConnectionInfoAssertions;
import org.neo4j.bolt.testing.assertions.ConnectionHandleAssertions;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.Level;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.time.FakeClock;

class AtomicSchedulingConnectionTest {

    private static final String CONNECTOR_ID = "bolt";
    private static final String CONNECTION_ID = "bolt-test";
    private static final long CONNECTION_TIME = 424242;
    private static final String USER_AGENT = "BoltTest/0.1.0 (+https://example.org)";
    private static final Map<String, String> BOLT_AGENT = Map.of("product", "neo4j-test/5.X");
    private static final String CLIENT_ADDRESS = "133.37.21.42:1337";
    private static final String SERVER_ADDRESS = "10.13.37.42:4949";
    private static final String DEFAULT_DB = "neo4j";
    private static final String AUTHENTICATED_USER = "bob";
    private static final String IMPERSONATED_USER = "alice";
    public static final String IMPERSONATED_DB = "foo";

    private ErrorAccountant errorAccountant;

    private Connector connector;
    private Channel channel;
    private MemoryTracker memoryTracker;
    private LogService logService;
    private AssertableLogProvider userLogProvider;
    private AssertableLogProvider internalLogProvider;
    private ExecutorService executorService;
    private FakeClock clock;

    private SocketAddress clientAddress;
    private SocketAddress serverAddress;
    private Authentication authentication;
    private DefaultDatabaseResolver defaultDatabaseResolver;

    private BoltProtocol protocol;
    private StateMachineConfiguration fsm;
    private StateMachine fsmInstance;
    private AuthenticationResult authenticationResult;
    private LoginContext loginContext;
    private AuthSubject authSubject;

    private AtomicSchedulingConnection connection;

    @BeforeEach
    void prepareConnection() {
        this.errorAccountant = Mockito.mock(ErrorAccountant.class);

        this.connector = Mockito.mock(Connector.class, Mockito.RETURNS_MOCKS);
        this.channel = Mockito.mock(Channel.class, Mockito.RETURNS_MOCKS);
        this.memoryTracker = Mockito.mock(MemoryTracker.class, Mockito.RETURNS_MOCKS);
        this.userLogProvider = new AssertableLogProvider();
        this.internalLogProvider = new AssertableLogProvider();
        this.logService = new SimpleLogService(this.userLogProvider, this.internalLogProvider);
        this.executorService = Mockito.mock(ExecutorService.class, Mockito.RETURNS_MOCKS);
        this.clock = new FakeClock();

        Mockito.doReturn(CONNECTOR_ID).when(this.connector).id();
        Mockito.doReturn(this.errorAccountant).when(this.connector).errorAccountant();

        this.clientAddress = Mockito.mock(SocketAddress.class);
        this.serverAddress = Mockito.mock(SocketAddress.class);
        this.authentication = Mockito.mock(Authentication.class, Mockito.RETURNS_MOCKS);
        this.defaultDatabaseResolver = Mockito.mock(DefaultDatabaseResolver.class);

        Mockito.doReturn(CLIENT_ADDRESS).when(this.clientAddress).toString();
        Mockito.doReturn(SERVER_ADDRESS).when(this.serverAddress).toString();
        Mockito.doReturn(DEFAULT_DB).when(this.defaultDatabaseResolver).defaultDatabase(ArgumentMatchers.any());

        Mockito.doReturn(this.clientAddress).when(this.channel).remoteAddress();
        Mockito.doReturn(this.serverAddress).when(this.channel).localAddress();
        Mockito.doReturn(this.authentication).when(this.connector).authentication();
        Mockito.doReturn(this.defaultDatabaseResolver).when(this.connector).defaultDatabaseResolver();

        this.protocol = Mockito.mock(BoltProtocol.class, Mockito.RETURNS_MOCKS);
        this.fsmInstance = Mockito.mock(StateMachine.class, Mockito.RETURNS_MOCKS);
        this.fsm = Mockito.mock(StateMachineConfiguration.class, Mockito.RETURNS_MOCKS);

        Mockito.doReturn(this.fsm).when(this.protocol).stateMachine();
        Mockito.doReturn(this.fsmInstance)
                .when(this.fsm)
                .createInstance(ArgumentMatchers.any(), ArgumentMatchers.any());

        this.authenticationResult = Mockito.mock(AuthenticationResult.class);
        this.loginContext = Mockito.mock(LoginContext.class, Mockito.RETURNS_MOCKS);
        this.authSubject = Mockito.mock(AuthSubject.class);

        Mockito.doReturn(this.loginContext).when(this.authenticationResult).getLoginContext();
        Mockito.doReturn(false).when(this.authenticationResult).credentialsExpired();

        Mockito.doReturn(this.authSubject).when(this.loginContext).subject();
        Mockito.doReturn(false).when(this.loginContext).impersonating();

        Mockito.doReturn(AUTHENTICATED_USER).when(this.authSubject).authenticatedUser();
        Mockito.doReturn(AUTHENTICATED_USER).when(this.authSubject).executingUser();

        this.connection = new AtomicSchedulingConnection(
                this.connector,
                CONNECTION_ID,
                this.channel,
                CONNECTION_TIME,
                this.memoryTracker,
                this.logService,
                this.executorService,
                this.clock);

        // this is to set user agent & bolt agent
        this.connection.negotiate(
                Collections.emptyList(),
                USER_AGENT,
                new RoutingContext(false, Collections.emptyMap()),
                null,
                BOLT_AGENT);
    }

    private void selectProtocol() {
        this.connection.selectProtocol(this.protocol);
    }

    private void authenticate() throws AuthenticationException {
        @SuppressWarnings("unchecked")
        var token = (Map<String, Object>) Mockito.mock(Map.class);

        Mockito.doReturn(this.authenticationResult)
                .when(this.authentication)
                .authenticate(ArgumentMatchers.eq(token), ArgumentMatchers.any());

        this.connection.logon(token);
    }

    @Test
    void shouldIdentifyConnector() {
        Assertions.assertThat(this.connection.connector()).isSameAs(this.connector);
    }

    @Test
    void shouldIdentifyConnectorId() {
        Assertions.assertThat(this.connection.connectorId()).isEqualTo(CONNECTOR_ID);

        Mockito.verify(this.connector).id();
        Mockito.verifyNoMoreInteractions(this.connector);
    }

    @Test
    void shouldIdentifyChannel() {
        Assertions.assertThat(this.connection.channel).isSameAs(this.channel);
    }

    @Test
    void shouldIdentifyId() {
        Assertions.assertThat(this.connection.id()).isEqualTo(CONNECTION_ID);
    }

    @Test
    void shouldIdentifyConnectionTime() {
        Assertions.assertThat(this.connection.connectTime()).isEqualTo(CONNECTION_TIME);
    }

    @Test
    void shouldIdentifyMemoryTracker() {
        Assertions.assertThat(this.connection.memoryTracker()).isSameAs(this.memoryTracker);

        Mockito.verifyNoMoreInteractions(this.connector);
    }

    @Test
    void shouldIdentifyServerAddress() {
        Assertions.assertThat(this.connection.serverAddress()).isEqualTo(this.serverAddress);

        Mockito.verify(this.channel).localAddress();
    }

    @Test
    void shouldIdentifyClientAddress() {
        Assertions.assertThat(this.connection.clientAddress()).isEqualTo(this.clientAddress);

        Mockito.verify(this.channel).remoteAddress();
    }

    @Test
    void shouldExecuteJobs() throws BrokenBarrierException, InterruptedException {
        this.selectProtocol();

        // newly created connections should consider themselves to be idle as there are no queued jobs nor have they
        // scheduled a task with their executor service
        ConnectionHandleAssertions.assertThat(this.connection)
                .isIdling()
                .hasNoPendingJobs()
                .notInWorkerThread()
                .isNotInterrupted()
                .isNotClosing()
                .isNotClosed();

        // however, once a job is submitted, they should consider themselves to be busy while their tasks remain pending
        var barrier = new CyclicBarrier(2);
        var latch = new CountDownLatch(1);
        var failure = new AtomicReference<AssertionError>();
        this.connection.submit((fsm, responseHandler) -> {
            try {
                barrier.await();

                try {
                    ConnectionHandleAssertions.assertThat(connection).inWorkerThread();
                } catch (AssertionError ex) {
                    failure.set(ex);
                }

                latch.await();
            } catch (BrokenBarrierException | InterruptedException ex) {
                throw new RuntimeException("Test interrupted", ex);
            }
        });

        ConnectionHandleAssertions.assertThat(this.connection)
                .isNotIdling()
                .hasPendingJobs()
                .notInWorkerThread()
                .isNotInterrupted()
                .isNotClosing()
                .isNotClosed();

        var runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(this.executorService).submit(runnableCaptor.capture());

        var runnable = runnableCaptor.getValue();
        Assertions.assertThat(runnable).isNotNull();

        // this should still be true if the job begins executing (it will no longer be considered to have pending jobs,
        // however)
        var thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.start();

        barrier.await(); // ensure that the job is actually running before we carry on

        ConnectionHandleAssertions.assertThat(this.connection)
                .isNotIdling()
                .hasNoPendingJobs()
                .notInWorkerThread();

        // once we allow the job to complete, the connection should return to idle
        latch.countDown();
        thread.join(); // ensure that the job has terminated cleanly

        ConnectionHandleAssertions.assertThat(this.connection)
                .isIdling()
                .hasNoPendingJobs()
                .notInWorkerThread();

        var innerFailure = failure.get();
        if (innerFailure != null) {
            throw innerFailure;
        }
    }

    @Test
    void shouldInterrupt() {
        this.selectProtocol();

        // newly created connections should never be interrupted
        ConnectionHandleAssertions.assertThat(this.connection).isNotInterrupted();

        // once interrupted the connection should identify itself as interrupted
        this.connection.interrupt();

        ConnectionHandleAssertions.assertThat(this.connection).isInterrupted();

        // interrupts can stack, if interrupted twice the connection should still identify itself as interrupted
        this.connection.interrupt();

        ConnectionHandleAssertions.assertThat(this.connection).isInterrupted();

        // since interrupts stack, the connection should remain interrupted when reset once
        Assertions.assertThat(this.connection.reset()).isFalse();

        ConnectionHandleAssertions.assertThat(this.connection).isInterrupted();

        // if reset another time, the second interrupt should clear and the connection should return to its normal state
        Assertions.assertThat(this.connection.reset()).isTrue();

        ConnectionHandleAssertions.assertThat(this.connection).isNotInterrupted();

        // if we interrupt it again, it should return to its interrupted state and identify itself as such
        this.connection.interrupt();

        ConnectionHandleAssertions.assertThat(this.connection).isInterrupted();

        // since there is only a single active interrupt, it should return to its normal state immediately when reset
        Assertions.assertThat(this.connection.reset()).isTrue();

        ConnectionHandleAssertions.assertThat(this.connection).isNotInterrupted();
    }

    @Test
    void shouldIdentifyClosingState() {
        this.selectProtocol();

        // submit an empty job to ensure that the connection doesn't inline close due to being in idle
        this.connection.submit((fsm, responseHandler) -> {});

        ConnectionHandleAssertions.assertThat(this.connection)
                .isActive()
                .isNotClosing()
                .isNotClosed();

        this.connection.close();

        // when the connection is still busy (e.g. is currently executing or has pending jobs), the connection should
        // simply be marked for closure but never actually close unless its jobs are executed
        ConnectionHandleAssertions.assertThat(this.connection)
                .isNotActive()
                .isClosing()
                .isNotClosed();
    }

    @Test
    void shouldImmediatelyCloseWhenInIdle() throws Exception {
        var future = Mockito.mock(ChannelFuture.class);
        var listener = Mockito.mock(ConnectionListener.class);

        Mockito.doReturn(future).when(this.channel).close();

        this.selectProtocol();

        ConnectionHandleAssertions.assertThat(this.connection)
                .isActive()
                .isNotClosing()
                .isNotClosed();

        this.connection.registerListener(listener);
        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        this.connection.close();

        // when closing in idle, the connection should immediately transition to closed as to not clog up the worker
        // pool without good reason
        ConnectionHandleAssertions.assertThat(this.connection)
                .isNotActive()
                .isNotClosing()
                .isClosed();

        // any closeable dependencies should also be closed once the connection is closed
        ArgumentCaptor<GenericFutureListener<? extends Future<? super Void>>> closeListenerCaptor =
                ArgumentCaptor.forClass(GenericFutureListener.class);

        Mockito.verify(this.channel).close();
        Mockito.verify(future).addListener(closeListenerCaptor.capture());

        Mockito.verify(this.memoryTracker, Mockito.never()).close();

        var closeListener = closeListenerCaptor.getValue();
        Assertions.assertThat(closeListener).isNotNull();

        closeListener.operationComplete(null);

        Mockito.verify(this.memoryTracker).close();

        // listeners should also be notified about both being marked for closure and actually closing
        Mockito.verify(listener).onMarkedForClosure();
        Mockito.verify(listener).onConnectionClosed(true);
        Mockito.verifyNoMoreInteractions(listener);

        // the close future should also be marked as completed as a result of this call so that dependent
        // components (such as ConnectionRegistry) can move on
        Assertions.assertThat(this.connection.closeFuture()).isDone();

        // the closure should also be logged correctly with the internal log to simplify debugging
        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(AtomicSchedulingConnection.class)
                .containsMessageWithArgumentsContaining("Closing connection", CONNECTION_ID);
    }

    @Test
    void shouldCloseFromWorkerThreadWhenScheduled() throws StateMachineException {
        var job1 = Mockito.mock(Job.class);
        var job2 = Mockito.mock(Job.class);

        var listener = Mockito.mock(ConnectionListener.class);

        this.selectProtocol();

        ConnectionHandleAssertions.assertThat(connection).isActive();

        // submit a job and capture the actual job on the executor service in order to delay the closure
        var runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        this.connection.submit(job1);
        Mockito.verify(this.executorService).submit(runnableCaptor.capture());

        ConnectionHandleAssertions.assertThat(connection).isActive();

        this.connection.submit(job2);
        Mockito.verifyNoMoreInteractions(this.executorService);

        ConnectionHandleAssertions.assertThat(connection).isActive();

        this.connection.registerListener(listener);
        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        var runnable = runnableCaptor.getValue();
        Assertions.assertThat(runnable).isNotNull();

        // if we now close the connection, it should be marked for closure but not immediately close as it still has
        // pending jobs
        this.connection.close();

        ConnectionHandleAssertions.assertThat(this.connection)
                .isNotActive()
                .isClosing()
                .isNotClosed();

        // if we invoke the submitted runnable, the connection should process the remaining jobs and finally close
        // itself once completed
        runnable.run();

        var inOrder = Mockito.inOrder(job1, job2, listener);

        inOrder.verify(listener).onMarkedForClosure();
        inOrder.verify(listener).onActivated();

        // since the jobs had not been started when the connection was marked for closure, they will not
        // execute - it is the callers responsibility to not kill connections which remain active if possible
        inOrder.verify(job1, Mockito.never()).perform(Mockito.eq(this.fsmInstance), Mockito.any());
        inOrder.verify(job2, Mockito.never()).perform(Mockito.eq(this.fsmInstance), Mockito.any());

        inOrder.verify(listener).onIdle(Mockito.anyLong());
        inOrder.verify(listener).onConnectionClosed(true);
        inOrder.verifyNoMoreInteractions();

        ConnectionHandleAssertions.assertThat(this.connection)
                .isNotActive()
                .isNotClosing()
                .isClosed();
    }

    @Test
    void shouldRegisterListeners() {
        var listener = Mockito.mock(ConnectionListener.class);

        // listeners should be notified about their addition to the connection
        this.connection.registerListener(listener);

        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);
    }

    @Test
    void shouldRemoveListeners() {
        var listener = Mockito.mock(ConnectionListener.class);

        this.connection.registerListener(listener);

        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        // listeners should be notified about their removal from the connection
        this.connection.removeListener(listener);

        Mockito.verify(listener).onListenerRemoved();
        Mockito.verifyNoMoreInteractions(listener);
    }

    @Test
    void shouldNotifyListeners() {
        var listener = Mockito.mock(ConnectionListener.class);

        this.connection.registerListener(listener);

        // listeners should be notified of the addition through their onListenerAdded function in order to initialize
        // their state prior to being invoked
        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        // listeners should receive calls through the notifyListeners API
        this.connection.notifyListeners(ConnectionListener::onActivated);

        Mockito.verify(listener).onActivated();
        Mockito.verifyNoMoreInteractions(listener);

        // listeners should also receive calls through the notifyListenersSafely API
        this.connection.notifyListenersSafely(
                "idle", connectionListener1 -> connectionListener1.onIdle(Mockito.anyLong()));

        Mockito.verify(listener).onIdle(Mockito.anyLong());
        Mockito.verifyNoMoreInteractions(listener);

        // when removed, listeners should be notified via the onListenerRemoved function
        this.connection.removeListener(listener);

        Mockito.verify(listener).onListenerRemoved();
        Mockito.verifyNoMoreInteractions(listener);

        // once removed, listeners should no longer receive notifications through either of the provided APIs
        this.connection.notifyListeners(connectionListener -> connectionListener.onConnectionClosed(true));

        Mockito.verifyNoMoreInteractions(listener);

        this.connection.notifyListenersSafely(
                "close", connectionListener -> connectionListener.onConnectionClosed(true));

        Mockito.verifyNoMoreInteractions(listener);
    }

    @Test
    void shouldNotThrowWhenSafelyNotifyingListeners() {
        var ex = new RuntimeException("This should not bubble up! :(");
        var throwingListener = Mockito.mock(ConnectionListener.class);
        var followingListener = Mockito.mock(ConnectionListener.class);

        Mockito.doThrow(ex).when(throwingListener).onActivated();

        this.connection.registerListener(throwingListener);
        this.connection.registerListener(followingListener);

        Mockito.verify(throwingListener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(throwingListener);
        Mockito.verify(followingListener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(followingListener);

        // notifyListenersSafely should invoke all listeners in order even when an exception is thrown (the resulting
        // exception does not bubble up and will be logged instead) - this functionality is primarily provided for the
        // purposes of invoking cleanup tasks which might fail depending on the current server state
        this.connection.notifyListenersSafely("activated", ConnectionListener::onActivated);

        Mockito.verify(throwingListener).onActivated();
        Mockito.verify(followingListener).onActivated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forClass(AtomicSchedulingConnection.class)
                .forLevel(AssertableLogProvider.Level.ERROR)
                .containsMessageWithException("Failed to publish activated event to listener", ex);
    }

    @Test
    void shouldIdentifyProtocol() {
        Assertions.assertThat(this.connection.protocol()).isNull();

        this.selectProtocol();

        Assertions.assertThat(this.connection.protocol()).isSameAs(this.protocol);
    }

    @Test
    void shouldSelectProtocol() {
        var listener = Mockito.mock(ConnectionListener.class);

        this.connection.registerListener(listener);
        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        this.selectProtocol();

        // a state machine should have been created for the selected protocol version
        Mockito.verify(this.protocol).stateMachine();
        Mockito.verify(this.fsm).createInstance(Mockito.eq(this.connection), Mockito.any());
        Mockito.verify(this.protocol).registerStructReaders(Mockito.any());
        Mockito.verify(this.protocol).registerStructWriters(Mockito.any());
        Mockito.verify(this.protocol).features();
        Mockito.verify(this.protocol).metadataHandler();
        Mockito.verify(this.protocol).onConnectionNegotiated(this.connection);
        Mockito.verifyNoMoreInteractions(this.fsm);
        Mockito.verifyNoMoreInteractions(this.protocol);

        // listeners should have been notified about the initialization of the state machine
        Mockito.verify(listener).onStateMachineInitialized(this.fsmInstance);
        Mockito.verifyNoMoreInteractions(listener);
    }

    @Test
    void selectProtocolShouldFailWithIllegalStateWhenInvokedTwice() {
        this.selectProtocol();
        Mockito.verify(this.protocol).stateMachine();
        Mockito.verify(this.fsm).createInstance(Mockito.eq(this.connection), Mockito.any());
        Mockito.verify(this.protocol).registerStructReaders(Mockito.any());
        Mockito.verify(this.protocol).registerStructWriters(Mockito.any());
        Mockito.verify(this.protocol).features();
        Mockito.verify(this.protocol).onConnectionNegotiated(this.connection);
        Mockito.verify(this.protocol).metadataHandler();
        Mockito.verifyNoMoreInteractions(this.protocol);

        Assertions.assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(this::selectProtocol)
                .withMessageContaining("Protocol has already been selected for connection")
                .withNoCause();

        Mockito.verifyNoMoreInteractions(this.protocol);
    }

    @Test
    void shouldIdentifyStateMachine() {
        this.selectProtocol();

        Assertions.assertThat(this.connection.fsm()).isSameAs(this.fsmInstance);
    }

    @Test
    void fsmShouldFailWithIllegalStateWhenNoProtocolHasBeenSelected() {
        Assertions.assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> this.connection.fsm())
                .withMessageContaining("Connection has yet to select a protocol version")
                .withNoCause();
    }

    @Test
    void shouldAuthenticate() throws AuthenticationException {
        @SuppressWarnings("unchecked")
        var token = (Map<String, Object>) Mockito.mock(Map.class);
        var result = Mockito.mock(AuthenticationResult.class);
        var loginContext = Mockito.mock(LoginContext.class);
        var subject = Mockito.mock(AuthSubject.class);
        var listener = Mockito.mock(ConnectionListener.class);

        Mockito.doReturn(result)
                .when(this.authentication)
                .authenticate(ArgumentMatchers.eq(token), ArgumentMatchers.any());

        Mockito.doReturn(loginContext).when(result).getLoginContext();
        Mockito.doReturn(false).when(result).credentialsExpired();

        Mockito.doReturn(subject).when(loginContext).subject();

        Mockito.doReturn(AUTHENTICATED_USER).when(subject).executingUser();
        Mockito.doReturn(AUTHENTICATED_USER).when(subject).authenticatedUser();

        this.connection.registerListener(listener);
        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        ConnectionHandleAssertions.assertThat(this.connection).isActive();

        Assertions.assertThat(this.connection.loginContext()).isNull();

        var flags = this.connection.logon(token);

        ConnectionHandleAssertions.assertThat(this.connection).isActive();

        // there should be no authentication flags set as the credentials were deemed valid at the time of
        // authentication
        Assertions.assertThat(flags).isNull();
        Mockito.verify(result, Mockito.times(2)).credentialsExpired(); // called two times due to logging

        // the client connection info should be updated to reflect the authenticated user within administrative
        // procedures such as list connection
        var clientInfoCaptor = ArgumentCaptor.forClass(ClientConnectionInfo.class);
        Mockito.verify(this.authentication).authenticate(ArgumentMatchers.eq(token), clientInfoCaptor.capture());

        var clientInfo = clientInfoCaptor.getValue();

        Assertions.assertThat(this.connection.userAgent()).isEqualTo(USER_AGENT);
        ClientConnectionInfoAssertions.assertThat(clientInfo)
                .hasProtocol(CONNECTOR_ID)
                .hasConnectionId(CONNECTION_ID)
                .hasClientAddress(CLIENT_ADDRESS)
                .hasRequestURI(SERVER_ADDRESS)
                .isSameAs(this.connection.info());

        Assertions.assertThat(this.connection.boltAgent()).isEqualTo(BOLT_AGENT);
        // the login context and username should have been updated to reflect the authentication result
        Assertions.assertThat(this.connection.loginContext()).isSameAs(loginContext);
        Assertions.assertThat(this.connection.username()).isEqualTo(AUTHENTICATED_USER);

        // the default database should have been resolved for the authenticated user
        Mockito.verify(this.defaultDatabaseResolver).defaultDatabase(AUTHENTICATED_USER);
        Assertions.assertThat(this.connection.selectedDefaultDatabase()).isEqualTo(DEFAULT_DB);

        // the listeners should have been notified about the newly authenticated user
        Mockito.verify(listener).onLogon(loginContext);

        // also make sure that the authentication is logged for debugging purposes
        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(AtomicSchedulingConnection.class)
                .containsMessageWithArgumentsContaining(
                        "Authenticated with user", CONNECTION_ID, AUTHENTICATED_USER, false);
    }

    @Test
    void shouldAuthenticateWithExpiredCredentials() throws AuthenticationException {
        @SuppressWarnings("unchecked")
        var token = (Map<String, Object>) Mockito.mock(Map.class);
        var result = Mockito.mock(AuthenticationResult.class);
        var loginContext = Mockito.mock(LoginContext.class, Mockito.RETURNS_MOCKS);
        var subject = Mockito.mock(AuthSubject.class);

        Mockito.doReturn(result)
                .when(this.authentication)
                .authenticate(ArgumentMatchers.eq(token), ArgumentMatchers.any());

        Mockito.doReturn(loginContext).when(result).getLoginContext();
        Mockito.doReturn(true).when(result).credentialsExpired();

        Mockito.doReturn(subject).when(loginContext).subject();

        Mockito.doReturn(AUTHENTICATED_USER).when(subject).executingUser();
        Mockito.doReturn(AUTHENTICATED_USER).when(subject).authenticatedUser();

        // the authentication flags should reflect the expired credentials
        var flags = this.connection.logon(token);

        Assertions.assertThat(flags).isEqualTo(AuthenticationFlag.CREDENTIALS_EXPIRED);

        // also make sure that the authentication is logged for debugging purposes
        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(AtomicSchedulingConnection.class)
                .containsMessageWithArgumentsContaining(
                        "Authenticated with user", CONNECTION_ID, AUTHENTICATED_USER, true);
    }

    @Test
    void authenticateShouldFailWithIllegalStateWhenInvokedTwice() throws AuthenticationException {
        this.authenticate();

        Assertions.assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(this::authenticate)
                .withMessage("Cannot re-authenticate connection")
                .withNoCause();
    }

    @Test
    void clientInfoShouldFailWithIllegalStatePreAuthentication() {
        Assertions.assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> this.connection.info())
                .withMessageContaining("has yet to be authenticated")
                .withNoCause();
    }

    @Test
    void shouldImpersonateUser() throws AuthenticationException {
        this.authenticate();

        var listener = Mockito.mock(ConnectionListener.class);
        var subject = Mockito.mock(AuthSubject.class);
        var impersonationContext = Mockito.mock(LoginContext.class);

        Mockito.doReturn(impersonationContext)
                .when(this.authentication)
                .impersonate(this.loginContext, IMPERSONATED_USER);

        Mockito.doReturn(subject).when(impersonationContext).subject();
        Mockito.doReturn(true).when(impersonationContext).impersonating();

        Mockito.doReturn(AUTHENTICATED_USER).when(subject).authenticatedUser();
        Mockito.doReturn(IMPERSONATED_USER).when(subject).executingUser();

        Mockito.doReturn(IMPERSONATED_DB).when(this.defaultDatabaseResolver).defaultDatabase(IMPERSONATED_USER);

        this.connection.registerListener(listener);
        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        Assertions.assertThat(this.connection.selectedDefaultDatabase()).isEqualTo(DEFAULT_DB);

        this.connection.impersonate(IMPERSONATED_USER);

        // an impersonation is performed using the previously stored login context
        Mockito.verify(this.authentication).impersonate(this.loginContext, IMPERSONATED_USER);

        // the indicated loginContext is replaced with the impersonated context for the duration of the impersonation so
        // that all executions scheduled through this connection make use of this authentication instead of the original
        Assertions.assertThat(this.connection.loginContext())
                .isNotSameAs(this.loginContext)
                .isSameAs(impersonationContext);

        // Connection username should remain on its original value as this is the value we report within administrative
        // commands (such as list connections) - as such, it should reflect the _actual_ user
        Assertions.assertThat(this.connection.username()).isEqualTo(AUTHENTICATED_USER);

        // listeners should be notified about the new impersonated login context
        Mockito.verify(listener).onUserImpersonated(impersonationContext);

        // the default database should be resolved again in order to switch to the impersonated user's home database if
        // any has been set
        Mockito.verify(this.defaultDatabaseResolver).defaultDatabase(IMPERSONATED_USER);
        Assertions.assertThat(this.connection.selectedDefaultDatabase()).isEqualTo(IMPERSONATED_DB);

        // also make sure that the impersonation is correctly logged to the debug log as this information will be
        // important when debugging error conditions
        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(AtomicSchedulingConnection.class)
                .containsMessageWithArgumentsContaining(
                        "Enabling impersonation of user", CONNECTION_ID, IMPERSONATED_USER);
    }

    @Test
    void shouldFailWithNullPointerExceptionWhenImpersonatedUserIsNull() throws AuthenticationException {
        this.authenticate();

        Mockito.verify(this.defaultDatabaseResolver).defaultDatabase(AUTHENTICATED_USER);
        Mockito.verifyNoMoreInteractions(this.defaultDatabaseResolver);

        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> this.connection.impersonate(null))
                .withMessage("userToImpersonate cannot be null")
                .withNoCause();
    }

    @Test
    void shouldClearImpersonatedUser() throws AuthenticationException {
        this.authenticate();

        Mockito.verify(this.defaultDatabaseResolver).defaultDatabase(AUTHENTICATED_USER);
        Mockito.verifyNoMoreInteractions(this.defaultDatabaseResolver);

        var listener = Mockito.mock(ConnectionListener.class);
        var subject = Mockito.mock(AuthSubject.class);
        var impersonationContext = Mockito.mock(LoginContext.class);

        Mockito.doReturn(impersonationContext)
                .when(this.authentication)
                .impersonate(this.loginContext, IMPERSONATED_USER);

        Mockito.doReturn(subject).when(impersonationContext).subject();
        Mockito.doReturn(true).when(impersonationContext).impersonating();

        Mockito.doReturn(AUTHENTICATED_USER).when(subject).authenticatedUser();
        Mockito.doReturn(IMPERSONATED_USER).when(subject).executingUser();

        Mockito.doReturn(IMPERSONATED_DB).when(this.defaultDatabaseResolver).defaultDatabase(IMPERSONATED_USER);

        this.connection.impersonate(IMPERSONATED_USER);

        Mockito.verify(this.defaultDatabaseResolver).defaultDatabase(IMPERSONATED_USER);
        Mockito.verifyNoMoreInteractions(this.defaultDatabaseResolver);

        this.connection.registerListener(listener);
        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        Assertions.assertThat(this.connection.selectedDefaultDatabase()).isEqualTo(IMPERSONATED_DB);

        this.connection.clearImpersonation();

        // the login context should return to the originally authenticated context
        Assertions.assertThat(this.connection.loginContext())
                .isNotSameAs(impersonationContext)
                .isSameAs(this.loginContext);

        // the default database should return to its previous value without being resolved again
        Mockito.verifyNoMoreInteractions(this.defaultDatabaseResolver);
        Assertions.assertThat(this.connection.selectedDefaultDatabase()).isEqualTo(DEFAULT_DB);

        // listeners should also be notified about the change of user as well as database
        Mockito.verify(listener).onDefaultDatabaseSelected(DEFAULT_DB);
        Mockito.verify(listener).onUserImpersonationCleared();
        Mockito.verifyNoMoreInteractions(listener);

        // the change should also be logged at debug level to allow debugging
        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(AtomicSchedulingConnection.class)
                .containsMessageWithArgumentsContaining("Disabling impersonation", CONNECTION_ID);
    }

    @Test
    void clearImpersonationShouldBeIgnoredWhenNoImpersonationIsActive() throws AuthenticationException {
        this.authenticate();

        Mockito.verify(this.defaultDatabaseResolver).defaultDatabase(AUTHENTICATED_USER);
        Mockito.verifyNoMoreInteractions(this.defaultDatabaseResolver);

        var listener = Mockito.mock(ConnectionListener.class);

        this.connection.registerListener(listener);
        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        this.connection.clearImpersonation();

        Mockito.verifyNoMoreInteractions(this.defaultDatabaseResolver);
        Mockito.verifyNoMoreInteractions(listener);
    }

    @Test
    void impersonateShouldFailWithIllegalStateWhenUnauthenticated() {
        var listener = Mockito.mock(ConnectionListener.class);

        this.connection.registerListener(listener);
        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        Assertions.assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> this.connection.impersonate(IMPERSONATED_USER))
                .withMessage("Cannot impersonate without prior authentication")
                .withNoCause();

        Mockito.verifyNoInteractions(this.defaultDatabaseResolver);
        Mockito.verifyNoMoreInteractions(listener);
    }

    @Test
    void shouldResolveDefaultDatabase() throws AuthenticationException {
        var listener = Mockito.mock(ConnectionListener.class);

        this.authenticate();

        this.connection.registerListener(listener);
        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        Assertions.assertThat(this.connection.selectedDefaultDatabase()).isEqualTo(DEFAULT_DB);

        Mockito.doReturn(IMPERSONATED_DB).when(this.defaultDatabaseResolver).defaultDatabase(AUTHENTICATED_USER);

        this.connection.resolveDefaultDatabase();

        // the selected database should switch to the new value immediately without caching
        Assertions.assertThat(this.connection.selectedDefaultDatabase()).isEqualTo(IMPERSONATED_DB);

        // the resolver should be used to acquire the new value
        Mockito.verify(this.defaultDatabaseResolver, Mockito.times(2)).defaultDatabase(AUTHENTICATED_USER);
        Mockito.verifyNoMoreInteractions(this.defaultDatabaseResolver);

        // listeners should also be notified about the new default database
        Mockito.verify(listener).onDefaultDatabaseSelected(IMPERSONATED_DB);
        Mockito.verifyNoMoreInteractions(listener);
    }

    @Test
    void resolveDefaultDatabaseShouldFailWithIllegalStateWhenUnauthenticated() {
        var listener = Mockito.mock(ConnectionListener.class);

        this.connection.registerListener(listener);
        Mockito.verify(listener).onListenerAdded();
        Mockito.verifyNoMoreInteractions(listener);

        Assertions.assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> this.connection.resolveDefaultDatabase())
                .withMessage("Cannot resolve default database: Connection has not been authenticated")
                .withNoCause();

        Mockito.verifyNoMoreInteractions(listener);
    }

    @Test
    void shouldReportNetworkErrors() throws AuthenticationException {
        this.selectProtocol();
        this.authenticate();

        var ex = new BoltStreamingWriteException("Test");
        this.connection.submit((fsm, responseHandler) -> {
            throw ex;
        });

        var captor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(this.executorService).submit(captor.capture());

        captor.getValue().run();

        Mockito.verify(this.errorAccountant).notifyNetworkAbort(this.connection, ex);
    }

    @Test
    void shouldReportSchedulingErrors() throws AuthenticationException {
        this.selectProtocol();
        this.authenticate();

        var ex = new RejectedExecutionException();
        Mockito.doThrow(ex).when(this.executorService).submit(Mockito.any(Runnable.class));

        this.connection.submit((fsm, responseHandler) -> {});

        Mockito.verify(this.errorAccountant).notifyThreadStarvation(this.connection, ex);
    }

    @Test
    void shouldServerErrorsAsErrors() throws AuthenticationException {
        this.selectProtocol();
        this.authenticate();

        this.connection.submit((fsm, responseHandler) -> {
            throw new RuntimeException("Test");
        });

        var captor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(this.executorService).submit(captor.capture());

        captor.getValue().run();

        LogAssertions.assertThat(this.userLogProvider)
                .forLevel(Level.ERROR)
                .forClass(AtomicSchedulingConnection.class)
                .containsMessages("Terminating connection due to unexpected error");
    }
}
