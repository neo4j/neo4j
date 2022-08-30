/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.protocol.common.transaction.result;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40.STREAM_LIMIT_UNLIMITED;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.response.SuccessMessage;
import org.neo4j.bolt.protocol.common.message.result.BoltResult;
import org.neo4j.bolt.protocol.io.DefaultBoltValueWriter;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

public class ResultHandlerTest {
    @Test
    void shouldCallHaltOnUnexpectedFailures() {
        // Given
        var ch = mock(Channel.class);
        doThrow(new RuntimeException("Something went horribly wrong"))
                .when(ch)
                .writeAndFlush(any(SuccessMessage.class));

        var connection = ConnectionMockFactory.newFactory().withChannel(ch).build();

        var handler = new ResultHandler(connection, DefaultBoltValueWriter::new, NullLogProvider.getInstance());

        // When
        handler.onFinish();

        // Then
        verify(connection).close();
    }

    @Test
    void shouldLogWriteErrorAndOriginalErrorWhenUnknownFailure() throws Exception {
        testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure(Error.from(new RuntimeException("Non-fatal error")));
    }

    @Test
    void shouldLogWriteErrorAndOriginalFatalErrorWhenUnknownFailure() throws Exception {
        testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure(Error.fatalFrom(new RuntimeException("Fatal error")));
    }

    @Test
    void shouldLogShortWarningOnClientDisconnectMidwayThroughQuery() throws Exception {
        // Connections dying is not exceptional per-se, so we don't need to fill the log with
        // eye-catching stack traces; but it could be indicative of some issue, so log a brief
        // warning in the debug log at least.

        // Given
        var outputClosed = new RuntimeException("UH OH!");
        Error txTerminated = Error.from(new TransactionTerminatedException(Status.Transaction.Terminated));

        // When
        AssertableLogProvider logProvider = emulateFailureWritingError(txTerminated, outputClosed);

        // Then
        assertThat(logProvider)
                .forClass(ResultHandler.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "Client %s disconnected while query was running. Session has been cleaned up. "
                                + "This can be caused by temporary network problems, but if you see this often, ensure your "
                                + "applications are properly waiting for operations to complete before exiting.",
                        new Object[] {null});
    }

    private static void testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure(Error original) throws Exception {
        RuntimeException outputError = new RuntimeException("Output failed");
        AssertableLogProvider logProvider = emulateFailureWritingError(original, outputError);

        assertThat(logProvider)
                .forClass(ResultHandler.class)
                .forLevel(WARN)
                .containsMessageWithException("Unable to send error back to the client", outputError);

        assertThat(outputError).hasSuppressedException(original.cause());
    }

    @Test
    void shouldPullTheResult() throws Throwable {
        var ch = mock(Channel.class);
        var connection = ConnectionMockFactory.newFactory().withChannel(ch).build();

        var handler = new ResultHandler(connection, DefaultBoltValueWriter::new, NullLogProvider.getInstance());

        var result = mock(BoltResult.class);

        handler.onPullRecords(result, STREAM_LIMIT_UNLIMITED);
        handler.onFinish();

        verify(result).handleRecords(any(BoltResult.RecordConsumer.class), eq(STREAM_LIMIT_UNLIMITED));
        verify(ch).writeAndFlush(any(SuccessMessage.class));
    }

    @Test
    void shouldDiscardTheResult() throws Throwable {
        var channel = mock(Channel.class);
        var connection = ConnectionMockFactory.newFactory().withChannel(channel).build();

        var handler = new ResultHandler(connection, DefaultBoltValueWriter::new, NullLogProvider.getInstance());

        var result = mock(BoltResult.class);

        handler.onDiscardRecords(result, STREAM_LIMIT_UNLIMITED);
        handler.onFinish();

        verify(result).discardRecords(any(BoltResult.DiscardingRecordConsumer.class), eq(STREAM_LIMIT_UNLIMITED));
        verify(channel).writeAndFlush(any(SuccessMessage.class));
    }

    private static AssertableLogProvider emulateFailureWritingError(Error error, Throwable exception) {
        var logProvider = new AssertableLogProvider();

        // I'm sorry ... I truly am ...
        var future = mock(ChannelFuture.class);

        doReturn(false).when(future).isSuccess();

        doReturn(exception).when(future).cause();

        doAnswer(call -> {
                    GenericFutureListener listener = call.getArgument(0);
                    listener.operationComplete((Future) call.getMock());

                    return call.getMock();
                })
                .when(future)
                .addListener(any());

        var ch = mock(Channel.class);

        doReturn(null).when(ch).remoteAddress();

        doReturn(future).when(ch).writeAndFlush(any());

        var connection = ConnectionMockFactory.newFactory().withChannel(ch).build();

        var handler = new ResultHandler(connection, DefaultBoltValueWriter::new, logProvider);

        handler.markFailed(error);
        handler.onFinish();

        return logProvider;
    }
}
