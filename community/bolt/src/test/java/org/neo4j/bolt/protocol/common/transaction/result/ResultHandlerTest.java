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
package org.neo4j.bolt.protocol.common.transaction.result;

public class ResultHandlerTest {
    //    @Test
    //    void shouldCallHaltOnUnexpectedFailures() {
    //        var future = Mockito.mock(ChannelFuture.class, Mockito.RETURNS_SELF);
    //        Mockito.doReturn(false).when(future).isSuccess();
    //        Mockito.doReturn(new RuntimeException("Something went horribly wrong"))
    //                .when(future)
    //                .cause();
    //
    //        var ch = mock(Channel.class);
    //
    //        var connection = ConnectionMockFactory.newFactory().withChannel(ch).build();
    //        Mockito.doReturn(future).when(connection).writeAndFlush(Mockito.any());
    //
    //        var handler = new ResultHandler(connection, NullLogProvider.getInstance());
    //
    //        Assertions.assertThatExceptionOfType(BoltStreamingWriteException.class)
    //                .isThrownBy(handler::onFinish)
    //                .withCauseExactlyInstanceOf(RuntimeException.class)
    //                .withMessage("Failed to finalize batch: Cannot write result response");
    //    }
    //
    //    @Test
    //    void shouldLogWriteErrorAndOriginalErrorWhenUnknownFailure() throws Exception {
    //        testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure(Error.from(new RuntimeException("Non-fatal
    // error")));
    //    }
    //
    //    @Test
    //    void shouldLogWriteErrorAndOriginalFatalErrorWhenUnknownFailure() throws Exception {
    //        testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure(Error.fatalFrom(new RuntimeException("Fatal
    // error")));
    //    }
    //
    //    @Test
    //    void shouldLogShortWarningOnClientDisconnectMidwayThroughQuery() throws Exception {
    //        // Connections dying is not exceptional per-se, so we don't need to fill the log with
    //        // eye-catching stack traces; but it could be indicative of some issue, so log a brief
    //        // warning in the debug log at least.
    //
    //        // Given
    //        var outputClosed = new RuntimeException("UH OH!");
    //        var txTerminated = Error.from(new TransactionTerminatedException(Status.Transaction.Terminated));
    //
    //        // When
    //        var logProvider = emulateFailureWritingError(txTerminated, outputClosed);
    //
    //        // Then
    //        assertThat(logProvider)
    //                .forClass(ResultHandler.class)
    //                .forLevel(WARN)
    //                .containsMessageWithArguments(
    //                        "Client %s disconnected while query was running. Session has been cleaned up. "
    //                                + "This can be caused by temporary network problems, but if you see this often,
    // ensure your "
    //                                + "applications are properly waiting for operations to complete before exiting.",
    //                        new Object[] {null});
    //    }
    //
    //    private static void testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure(Error original) throws Exception
    // {
    //        RuntimeException outputError = new RuntimeException("Output failed");
    //        AssertableLogProvider logProvider = emulateFailureWritingError(original, outputError);
    //
    //        assertThat(logProvider)
    //                .forClass(ResultHandler.class)
    //                .forLevel(WARN)
    //                .containsMessageWithException("Unable to send error back to the client", outputError);
    //
    //        assertThat(outputError).hasSuppressedException(original.cause());
    //    }
    //
    //    @Test
    //    void shouldPullTheResult() throws Throwable {
    //        var future = mock(ChannelFuture.class, Mockito.RETURNS_SELF);
    //        Mockito.doReturn(true).when(future).isSuccess();
    //
    //        var connection = ConnectionMockFactory.newFactory().build();
    //        Mockito.doReturn(future).when(connection).writeAndFlush(Mockito.any());
    //
    //        Mockito.doReturn(future).when(connection).writeAndFlush(Mockito.any());
    //
    //        var handler = new ResultHandler(connection, NullLogProvider.getInstance());
    //
    //        var result = mock(BoltResult.class);
    //
    //        handler.onPullRecords(result, STREAM_LIMIT_UNLIMITED);
    //        handler.onFinish();
    //
    //        verify(result).handleRecords(any(BoltResult.RecordConsumer.class), eq(STREAM_LIMIT_UNLIMITED));
    //        verify(connection).writeAndFlush(any(SuccessMessage.class));
    //        verify(future).sync();
    //        verify(future).isSuccess();
    //    }
    //
    //    @Test
    //    void shouldDiscardTheResult() throws Throwable {
    //        var future = mock(ChannelFuture.class, Mockito.RETURNS_SELF);
    //        Mockito.doReturn(true).when(future).isSuccess();
    //
    //        var connection = ConnectionMockFactory.newFactory().build();
    //        Mockito.doReturn(future).when(connection).writeAndFlush(Mockito.any());
    //
    //        var handler = new ResultHandler(connection, NullLogProvider.getInstance());
    //
    //        var result = mock(BoltResult.class);
    //
    //        handler.onDiscardRecords(result, STREAM_LIMIT_UNLIMITED);
    //        handler.onFinish();
    //
    //        verify(result).discardRecords(any(BoltResult.DiscardingRecordConsumer.class), eq(STREAM_LIMIT_UNLIMITED));
    //        verify(connection).writeAndFlush(any(SuccessMessage.class));
    //        verify(future).sync();
    //        verify(future).isSuccess();
    //    }
    //
    //    private static AssertableLogProvider emulateFailureWritingError(Error error, Throwable exception) {
    //        var logProvider = new AssertableLogProvider();
    //
    //        // I'm sorry ... I truly am ...
    //        var future = mock(ChannelFuture.class, Mockito.RETURNS_SELF);
    //
    //        doReturn(false).when(future).isSuccess();
    //
    //        doReturn(exception).when(future).cause();
    //
    //        doAnswer(call -> {
    //                    GenericFutureListener listener = call.getArgument(0);
    //                    listener.operationComplete((Future) call.getMock());
    //
    //                    return call.getMock();
    //                })
    //                .when(future)
    //                .addListener(any());
    //
    //        var connection = ConnectionMockFactory.newFactory().build();
    //
    //        Mockito.doReturn(null).when(connection).clientAddress();
    //        Mockito.doReturn(future).when(connection).writeAndFlush(Mockito.any());
    //
    //        var handler = new ResultHandler(connection, logProvider);
    //
    //        handler.markFailed(error);
    //        handler.onFinish();
    //
    //        return logProvider;
    //    }
}
