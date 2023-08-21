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
package org.neo4j.bolt.protocol.v40.fsm;

class StateMachineV40Test {
    //    @Test
    //    void initialStateShouldBeConnected() {
    //        assertThat(newMachine()).isInState(ConnectedState.class);
    //    }
    //
    //    @Test
    //    void shouldRollbackOpenTransactionOnReset() throws Throwable {
    //        // Given a FAILED machine with an open transaction
    //        var interruptCounter = new AtomicInteger();
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withInterruptedCaptor(interruptCounter)
    //                .build();
    //        var machine = newMachine(connection);
    //        initTransaction(machine);
    //        machine.markFailed(Error.from(new RuntimeException()));
    //
    //        // When RESET occurs
    //        reset(connection, machine, nullResponseHandler());
    //
    //        // Then ...
    //        assertThat(machine).doesNotHaveTransaction().isInState(ReadyState.class);
    //
    //        Assertions.assertThat(interruptCounter).hasValue(0);
    //    }
    //
    //    @Test
    //    void shouldRollbackOpenTransactionOnClose() throws Throwable {
    //        // Given a ready machine with an open transaction
    //        var machine = newMachine();
    //        initTransaction(machine);
    //
    //        // When the machine is shut down
    //        machine.close();
    //
    //        // Then the transaction should have been rolled back
    //        assertThat(machine).doesNotHaveTransaction();
    //    }
    //
    //    @Test
    //    void shouldBeAbleToResetWhenInReadyState() throws Throwable {
    //        var interruptCounter = new AtomicInteger();
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withInterruptedCaptor(interruptCounter)
    //                .build();
    //        var machine = init(newMachine(connection));
    //
    //        assertThat(machine).canReset(connection).doesNotHaveTransaction();
    //
    //        Assertions.assertThat(interruptCounter).hasValue(0);
    //    }
    //
    //    @Test
    //    void shouldResetWithOpenTransaction() throws Throwable {
    //        var interruptCounter = new AtomicInteger();
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withInterruptedCaptor(interruptCounter)
    //                .build();
    //        var machine = newMachine(connection);
    //        initTransaction(machine);
    //
    //        assertThat(machine).canReset(connection).doesNotHaveTransaction();
    //
    //        Assertions.assertThat(interruptCounter).hasValue(0);
    //    }
    //
    //    @Test
    //    void shouldResetWithOpenTransactionAndOpenResult() throws Throwable {
    //        // Given a ready machine with an open transaction...
    //        var interruptCounter = new AtomicInteger();
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withInterruptedCaptor(interruptCounter)
    //                .build();
    //        var machine = newMachine(connection);
    //        initTransaction(machine);
    //
    //        // ...and an open result
    //        machine.process(BoltV40Messages.run(), nullResponseHandler());
    //
    //        // Then
    //        assertThat(machine).canReset(connection).doesNotHaveTransaction();
    //
    //        Assertions.assertThat(interruptCounter).hasValue(0);
    //    }
    //
    //    @Test
    //    void shouldResetWithOpenResult() throws Throwable {
    //        // Given a ready machine...
    //        var interruptCounter = new AtomicInteger();
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withInterruptedCaptor(interruptCounter)
    //                .build();
    //
    //        var machine = init(newMachine(connection));
    //
    //        // ...and an open result
    //        machine.process(BoltV40Messages.run(), nullResponseHandler());
    //
    //        // Then
    //        assertThat(machine).canReset(connection).doesNotHaveTransaction();
    //
    //        Assertions.assertThat(interruptCounter).hasValue(0);
    //    }
    //
    //    @Test
    //    void shouldFailWhenOutOfOrderRollback() throws Throwable {
    //        // Given a failed machine
    //        var machine = newMachine();
    //        machine.markFailed(Error.from(new RuntimeException()));
    //
    //        // When
    //        machine.process(BoltV40Messages.rollback(), nullResponseHandler());
    //
    //        // Then
    //        assertThat(machine).isInState(FailedState.class);
    //    }
    //
    //    @Test
    //    void shouldRemainStoppedAfterInterrupted() throws Throwable {
    //        // Given a ready machine
    //        var connection = ConnectionMockFactory.newInstance();
    //        var machine = init(newMachine(connection));
    //
    //        // ...which is subsequently closed
    //        machine.close();
    //
    //        // ...then it should indicate that it has been closed
    //        assertThat(machine).isClosed();
    //
    //        // When and interrupt and reset occurs
    //        reset(connection, machine, nullResponseHandler());
    //
    //        // ...then the machine should remain closed
    //        assertThat(machine).isClosed();
    //    }
    //
    //    @Test
    //    void shouldBeAbleToKillMessagesAheadInLineWithAnInterrupt() throws Throwable {
    //        // Given
    //        var interruptCounter = new AtomicInteger(0);
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withInterruptedCaptor(interruptCounter)
    //                .build();
    //        var machine = init(newMachine(connection));
    //
    //        // When
    //        interruptCounter.set(1);
    //        machine.interrupt();
    //
    //        // ...and
    //        var recorder = new ResponseRecorder();
    //        machine.process(BoltV40Messages.run(), recorder);
    //        machine.process(BoltV40Messages.reset(), recorder);
    //        machine.process(BoltV40Messages.run(), recorder);
    //
    //        // Then
    //        assertThat(recorder).hasIgnoredResponse().hasSuccessResponse(2);
    //
    //        Assertions.assertThat(interruptCounter).hasValue(0);
    //    }
    //
    //    @Test
    //    void multipleInterruptsShouldBeMatchedWithMultipleResets() throws Throwable {
    //        // Given
    //        var interruptCounter = new AtomicInteger();
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withInterruptedCaptor(interruptCounter)
    //                .build();
    //        var machine = init(newMachine(connection));
    //
    //        // When
    //        interruptCounter.set(2);
    //        machine.interrupt();
    //        machine.interrupt();
    //
    //        // ...and
    //        var recorder = new ResponseRecorder();
    //        machine.process(BoltV40Messages.run(), recorder);
    //        machine.process(BoltV40Messages.reset(), recorder);
    //        machine.process(BoltV40Messages.run(), recorder);
    //
    //        // Then
    //        assertThat(recorder).hasIgnoredResponse(3);
    //
    //        // But when
    //        recorder.reset();
    //        machine.process(BoltV40Messages.reset(), recorder);
    //        machine.process(BoltV40Messages.run(), recorder);
    //
    //        // Then
    //        assertThat(recorder).hasSuccessResponse(2);
    //
    //        Assertions.assertThat(interruptCounter).hasValue(0);
    //    }
    //
    //    @Test
    //    void testPublishingError() throws Throwable {
    //        // Given a new ready machine...
    //        var machine = init(newMachine());
    //
    //        // ...and a result ready to be retrieved...
    //        machine.process(BoltV40Messages.run(), nullResponseHandler());
    //
    //        // ...and a handler guaranteed to break
    //        ResponseRecorder recorder = new ResponseRecorder() {
    //            @Override
    //            public boolean onPullRecords(BoltResult result, long size) {
    //                throw new RuntimeException("I've been expecting you, Mr Bond.");
    //            }
    //        };
    //
    //        // When we pull using that handler
    //        machine.process(BoltV40Messages.pull(), recorder);
    //
    //        // Then the breakage should surface as a FAILURE
    //        assertThat(recorder).hasFailureResponse(Status.General.UnknownError);
    //
    //        // ...and the machine should have entered a FAILED state
    //        assertThat(machine).isInState(FailedState.class);
    //    }
    //
    //    @Test
    //    void testRollbackError() throws Throwable {
    //        // Given
    //        var machine = init(newMachineWithMockedTxManager());
    //
    //        // Given there is a running transaction
    //        machine.process(BoltV40Messages.begin(), nullResponseHandler());
    //
    //        // And given that transaction will fail to roll back
    //        doThrow(new TransactionFailureException("No Mr. Bond, I expect you to die."))
    //                .when(((AbstractStateMachine) machine).transactionManager())
    //                .rollback(any());
    //
    //        // When
    //        machine.process(BoltV40Messages.rollback(), nullResponseHandler());
    //
    //        // Then
    //        assertThat(machine).isInState(FailedState.class);
    //    }
    //
    //    @Test
    //    void testFailOnNestedTransactions() throws Throwable {
    //        // Given
    //        var machine = init(newMachine());
    //
    //        // Given there is a running transaction
    //        machine.process(BoltV40Messages.begin(), nullResponseHandler());
    //
    //        // When
    //        assertThrows(
    //                BoltProtocolBreachFatality.class,
    //                () -> machine.process(BoltV40Messages.begin(), nullResponseHandler()));
    //
    //        // Then
    //        assertThat(machine).isInInvalidState();
    //    }
    //
    //    @Test
    //    void testCantDoAnythingIfInFailedState() throws Throwable {
    //        // Given a FAILED machine
    //        var machine = init(newMachine());
    //        machine.markFailed(Error.from(new RuntimeException()));
    //
    //        // Then no RUN...
    //        machine.process(BoltV40Messages.run(), nullResponseHandler());
    //
    //        assertThat(machine).isInState(FailedState.class);
    //
    //        // ...DISCARD_ALL...
    //        machine.process(BoltV40Messages.discard(), nullResponseHandler());
    //
    //        assertThat(machine).isInState(FailedState.class);
    //
    //        // ...or PULL_ALL should be possible
    //        machine.process(BoltV40Messages.pull(), nullResponseHandler());
    //
    //        assertThat(machine).isInState(FailedState.class);
    //    }
    //
    //    @Test
    //    void testUsingResetToAcknowledgeError() throws Throwable {
    //        // Given
    //        var recorder = new ResponseRecorder();
    //
    //        // Given a FAILED machine
    //        var interruptCounter = new AtomicInteger();
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withInterruptedCaptor(interruptCounter)
    //                .build();
    //        var machine = init(newMachine(connection));
    //        machine.markFailed(Error.from(new RuntimeException()));
    //
    //        // When I RESET...
    //        reset(connection, machine, recorder);
    //
    //        // ...successfully
    //        assertThat(recorder).hasSuccessResponse();
    //
    //        // Then if I RUN a statement...
    //        machine.process(BoltV40Messages.run(), recorder);
    //
    //        // ...everything should be fine again
    //        assertThat(recorder).hasSuccessResponse();
    //    }
    //
    //    @Test
    //    void actionsDisallowedBeforeInitialized() {
    //        // Given
    //        var machine = newMachine();
    //
    //        // When
    //        try {
    //            machine.process(BoltV40Messages.run(), nullResponseHandler());
    //            fail("Failed to fail fatally");
    //        }
    //
    //        // Then
    //        catch (BoltConnectionFatality e) {
    //            // fatality correctly generated
    //        }
    //    }
    //
    //    @Test
    //    void shouldTerminateOnAuthExpiryDuringREADYRun() throws Throwable {
    //        // Given
    //        var transactionSPI = mock(TransactionStateMachineSPI.class);
    //        doThrow(new AuthorizationExpiredException("Auth expired!"))
    //                .when(transactionSPI)
    //                .beginTransaction(any(), any(), any(), any(), any(), any(), any());
    //
    //        var machine = newMachineWithTransactionSPI(transactionSPI);
    //
    //        // When & Then
    //        try {
    //            machine.process(BoltV40Messages.run("THIS WILL BE IGNORED"), nullResponseHandler());
    //            fail("Exception expected");
    //        } catch (BoltConnectionAuthFatality e) {
    //            assertEquals("Auth expired!", e.getCause().getMessage());
    //        }
    //    }
    //
    //    @Test
    //    void shouldTerminateOnAuthExpiryDuringSTREAMINGPullAll() throws Throwable {
    //        // Given
    //        var responseHandler = mock(ResponseHandler.class);
    //        doThrow(new AuthorizationExpiredException("Auth expired!"))
    //                .when(responseHandler)
    //                .onPullRecords(any(), eq(STREAM_LIMIT_UNLIMITED));
    //
    //        var machine = init(newMachine());
    //        machine.process(BoltV40Messages.run(), nullResponseHandler()); // move to streaming state
    //
    //        // When & Then
    //        try {
    //            machine.process(BoltV40Messages.pull(), responseHandler);
    //            fail("Exception expected");
    //        } catch (BoltConnectionAuthFatality e) {
    //            assertEquals("Auth expired!", e.getCause().getMessage());
    //        }
    //
    //        verify(responseHandler).onPullRecords(any(), eq(STREAM_LIMIT_UNLIMITED));
    //    }
    //
    //    @Test
    //    void shouldTerminateOnAuthExpiryDuringSTREAMINGDiscardAll() throws Throwable {
    //        // Given
    //        var responseHandler = mock(ResponseHandler.class);
    //        doThrow(new AuthorizationExpiredException("Auth expired!"))
    //                .when(responseHandler)
    //                .onDiscardRecords(any(), eq(STREAM_LIMIT_UNLIMITED));
    //
    //        var machine = init(newMachine());
    //        machine.process(BoltV40Messages.run(), nullResponseHandler()); // move to streaming state
    //
    //        // When & Then
    //        try {
    //            machine.process(BoltV40Messages.discard(), responseHandler);
    //            fail("Exception expected");
    //        } catch (BoltConnectionAuthFatality e) {
    //            assertEquals("Auth expired!", e.getCause().getMessage());
    //        }
    //    }
    //
    //    @Test
    //    void shouldSetPendingErrorOnMarkFailedIfNoHandler() {
    //        var spi = mock(StateMachineSPIImpl.class);
    //        var connection = ConnectionMockFactory.newInstance();
    //
    //        var machine = new StateMachineV40(
    //                spi,
    //                connection,
    //                Clock.systemUTC(),
    //                mock(DefaultDatabaseResolver.class),
    //                mock(TransactionManager.class));
    //
    //        var error = Error.from(Status.Request.NoThreadsAvailable, "no threads");
    //
    //        machine.markFailed(error);
    //
    //        assertEquals(error, pendingError(machine));
    //
    //        assertThat(machine).isInState(FailedState.class);
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextInitMessageOnMarkFailedIfNoHandler() throws Exception {
    //        testMarkFailedOnNextMessage((machine, handler) -> machine.process(BoltV40Messages.hello(), handler));
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextRunMessageOnMarkFailedIfNoHandler() throws Exception {
    //        testMarkFailedOnNextMessage((machine, handler) -> machine.process(BoltV40Messages.run(), handler));
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextPullAllMessageOnMarkFailedIfNoHandler() throws Exception {
    //        testMarkFailedOnNextMessage((machine, handler) -> machine.process(BoltV40Messages.pull(), handler));
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextDiscardAllMessageOnMarkFailedIfNoHandler() throws Exception {
    //        testMarkFailedOnNextMessage((machine, handler) -> machine.process(BoltV40Messages.discard(), handler));
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextResetMessageOnMarkFailedIfNoHandler() throws Exception {
    //        // Given
    //        var interruptCounter = new AtomicInteger();
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withInterruptedCaptor(interruptCounter)
    //                .build();
    //        var machine = init(newMachine(connection));
    //        var responseHandler = mock(ResponseHandler.class);
    //
    //        var error = Error.from(Status.Request.NoThreadsAvailable, "no threads");
    //        machine.markFailed(error);
    //
    //        // When
    //        reset(connection, machine, responseHandler);
    //
    //        // Expect
    //        assertNull(pendingError(machine));
    //        assertFalse(pendingIgnore(machine));
    //
    //        assertThat(machine).isInState(ReadyState.class);
    //
    //        verify(responseHandler, never()).markFailed(any());
    //        verify(responseHandler, never()).markIgnored();
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextExternalErrorMessageOnMarkFailedIfNoHandler() throws Exception {
    //        testMarkFailedOnNextMessage((machine, handler) ->
    //                machine.handleExternalFailure(Error.from(Status.Request.Invalid, "invalid"), handler));
    //    }
    //
    //    @Test
    //    void shouldSetPendingIgnoreOnMarkFailedIfAlreadyFailedAndNoHandler() {
    //        var machine = newMachine();
    //
    //        var error1 = Error.from(new RuntimeException());
    //        machine.markFailed(error1);
    //
    //        var error2 = Error.from(Status.Request.NoThreadsAvailable, "no threads");
    //        machine.markFailed(error2);
    //
    //        assertTrue(pendingIgnore(machine));
    //        assertEquals(error1, pendingError(machine)); // error remained the same and was ignored
    //
    //        assertThat(machine).isInState(FailedState.class);
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextInitMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception {
    //        testMarkFailedShouldYieldIgnoredIfAlreadyFailed(
    //                (machine, handler) -> machine.process(BoltV40Messages.hello(), handler));
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextRunMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception {
    //        testMarkFailedShouldYieldIgnoredIfAlreadyFailed(
    //                (machine, handler) -> machine.process(BoltV40Messages.run(), handler));
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextPullAllMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception
    // {
    //        testMarkFailedShouldYieldIgnoredIfAlreadyFailed(
    //                (machine, handler) -> machine.process(BoltV40Messages.pull(), handler));
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextDiscardAllMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws
    // Exception {
    //        testMarkFailedShouldYieldIgnoredIfAlreadyFailed(
    //                (machine, handler) -> machine.process(BoltV40Messages.discard(), handler));
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextResetMessageOnMarkFailedIfAlreadyFailedAndNoHandler() throws Exception {
    //        // Given
    //        var interruptCounter = new AtomicInteger();
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withInterruptedCaptor(interruptCounter)
    //                .build();
    //        var machine = init(newMachine(connection));
    //        machine.markFailed(Error.from(new RuntimeException()));
    //
    //        var responseHandler = mock(ResponseHandler.class);
    //
    //        var error = Error.from(Status.Request.NoThreadsAvailable, "no threads");
    //        machine.markFailed(error);
    //
    //        // When
    //        reset(connection, machine, responseHandler);
    //
    //        // Expect
    //        assertNull(pendingError(machine));
    //        assertFalse(pendingIgnore(machine));
    //
    //        assertThat(machine).isInState(ReadyState.class);
    //
    //        verify(responseHandler, never()).markIgnored();
    //        verify(responseHandler, never()).markFailed(any());
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnNextExternalErrorMessageOnMarkFailedIfAlreadyFailedAndNoHandler()
    //            throws Exception {
    //        testMarkFailedShouldYieldIgnoredIfAlreadyFailed((machine, handler) ->
    //                machine.handleExternalFailure(Error.from(Status.Request.Invalid, "invalid"), handler));
    //    }
    //
    //    @Test
    //    void shouldInvokeResponseHandlerOnMarkFailedIfThereIsHandler() throws Exception {
    //        var machine = init(newMachine());
    //        var error = Error.from(Status.Request.NoThreadsAvailable, "no threads");
    //
    //        var responseHandler = mock(ResponseHandler.class);
    //        ((AbstractStateMachine) machine).connectionState().setResponseHandler(responseHandler);
    //        machine.markFailed(error);
    //
    //        assertNull(pendingError(machine));
    //        assertFalse(pendingIgnore(machine));
    //
    //        assertThat(machine).isInState(FailedState.class);
    //
    //        verify(responseHandler).markFailed(error);
    //    }
    //
    //    @Test
    //    void shouldNotFailWhenMarkedForTerminationAndPullAll() throws Exception {
    //        var spi = mock(StateMachineSPIImpl.class, RETURNS_MOCKS);
    //
    //        var machine = init(newMachine(spi));
    //        machine.process(BoltV40Messages.run(), nullResponseHandler()); // move to streaming state
    //
    //        var responseHandler = mock(ResponseHandler.class);
    //
    //        machine.markForTermination();
    //        machine.process(BoltV40Messages.pull(), responseHandler);
    //
    //        verify(spi, never()).reportError(any());
    //
    //        assertThat(machine).isNotInState(FailedState.class);
    //    }
    //
    //    @Test
    //    void shouldSucceedOnResetOnFailedState() throws Exception {
    //        // Given
    //        var recorder = new ResponseRecorder();
    //
    //        // Given a FAILED machine
    //        var interruptCounter = new AtomicInteger(0);
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withInterruptedCaptor(interruptCounter)
    //                .build();
    //        var machine = init(newMachine(connection));
    //
    //        machine.markFailed(Error.from(Status.Request.NoThreadsAvailable, "No Threads Available"));
    //        machine.process(BoltV40Messages.pull(), recorder);
    //
    //        // When I RESET...
    //        interruptCounter.incrementAndGet();
    //        machine.interrupt();
    //
    //        machine.markFailed(Error.from(Status.Request.NoThreadsAvailable, "No Threads Available"));
    //        machine.process(BoltV40Messages.reset(), recorder);
    //
    //        assertThat(recorder)
    //                .hasFailureResponse(Status.Request.NoThreadsAvailable)
    //                .hasSuccessResponse();
    //
    //        Assertions.assertThat(interruptCounter).hasValue(0);
    //    }
    //
    //    @Test
    //    void shouldSucceedOnConsecutiveResetsOnFailedState() throws Exception {
    //        // Given
    //        var recorder = new ResponseRecorder();
    //
    //        // Given a FAILED machine
    //        var captor = new AtomicInteger(0);
    //        var connection =
    //                ConnectionMockFactory.newFactory().withInterruptedCaptor(captor).build();
    //        var machine = init(newMachine(connection));
    //
    //        machine.markFailed(Error.from(Status.Request.NoThreadsAvailable, "No Threads Available"));
    //        machine.process(BoltV40Messages.pull(), recorder);
    //
    //        // When I RESET...
    //        captor.addAndGet(2);
    //        machine.interrupt();
    //        machine.interrupt();
    //
    //        machine.markFailed(Error.from(Status.Request.NoThreadsAvailable, "No Threads Available"));
    //        machine.process(BoltV40Messages.reset(), recorder);
    //        machine.markFailed(Error.from(Status.Request.NoThreadsAvailable, "No Threads Available"));
    //        machine.process(BoltV40Messages.reset(), recorder);
    //
    //        assertThat(recorder)
    //                .hasFailureResponse(Status.Request.NoThreadsAvailable)
    //                .hasIgnoredResponse()
    //                .hasSuccessResponse();
    //    }
    //
    //    @Test
    //    void shouldAllocateMemoryForStates() {
    //        var spi = mock(StateMachineSPIImpl.class);
    //        var memoryTracker = mock(MemoryTracker.class);
    //
    //        var connection = ConnectionMockFactory.newFactory()
    //                .withMemoryTracker(memoryTracker)
    //                .build();
    //
    //        // state allocation is a side effect of construction
    //        new StateMachineV40(
    //                spi,
    //                connection,
    //                Clock.systemUTC(),
    //                mock(DefaultDatabaseResolver.class),
    //                mock(TransactionManager.class));
    //
    //        verify(memoryTracker)
    //                .allocateHeap(ConnectedState.SHALLOW_SIZE
    //                        + ReadyState.SHALLOW_SIZE
    //                        + AutoCommitState.SHALLOW_SIZE
    //                        + InTransactionState.SHALLOW_SIZE
    //                        + FailedState.SHALLOW_SIZE
    //                        + InterruptedState.SHALLOW_SIZE);
    //    }
    //
    //    private static void testMarkFailedOnNextMessage(
    //            ThrowingBiConsumer<StateMachine, ResponseHandler, BoltConnectionFatality> action) throws Exception {
    //        // Given
    //        var machine = init(newMachine());
    //        var responseHandler = mock(ResponseHandler.class);
    //
    //        var error = Error.from(Status.Request.NoThreadsAvailable, "no threads");
    //        machine.markFailed(error);
    //
    //        // When
    //        action.accept(machine, responseHandler);
    //
    //        // Expect
    //        assertNull(pendingError(machine));
    //        assertFalse(pendingIgnore(machine));
    //
    //        assertThat(machine).isInState(FailedState.class);
    //
    //        verify(responseHandler).markFailed(error);
    //    }
    //
    //    private static void testReadyStateAfterMarkFailedOnNextMessage(
    //            ThrowingBiConsumer<StateMachine, ResponseHandler, BoltConnectionFatality> action) throws Exception {}
    //
    //    private static void testMarkFailedShouldYieldIgnoredIfAlreadyFailed(
    //            ThrowingBiConsumer<StateMachine, ResponseHandler, BoltConnectionFatality> action) throws Exception {
    //        // Given
    //        var machine = init(newMachine());
    //        machine.markFailed(Error.from(new RuntimeException()));
    //
    //        var responseHandler = mock(ResponseHandler.class);
    //
    //        var error = Error.from(Status.Request.NoThreadsAvailable, "no threads");
    //        machine.markFailed(error);
    //
    //        // When
    //        action.accept(machine, responseHandler);
    //
    //        // Expect
    //        assertNull(pendingError(machine));
    //        assertFalse(pendingIgnore(machine));
    //
    //        assertThat(machine).isInState(FailedState.class);
    //
    //        verify(responseHandler).markIgnored();
    //    }
    //
    //    private static void testMarkFailedShouldYieldSuccessIfAlreadyFailed(
    //            ThrowingBiConsumer<StateMachine, ResponseHandler, BoltConnectionFatality> action) throws Exception {}
    //
    //    private static Error pendingError(StateMachine machine) {
    //        return ((AbstractStateMachine) machine).connectionState().getPendingError();
    //    }
    //
    //    private static boolean pendingIgnore(StateMachine machine) {
    //        return ((AbstractStateMachine) machine).connectionState().hasPendingIgnore();
    //    }
}
