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
package org.neo4j.bolt.protocol.common.fsm.response;

import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.response.FailureMessage;
import org.neo4j.bolt.protocol.common.message.response.IgnoredMessage;
import org.neo4j.bolt.protocol.common.message.response.SuccessMessage;
import org.neo4j.bolt.testing.assertions.FailureMessageAssertions;
import org.neo4j.bolt.testing.assertions.MapValueAssertions;
import org.neo4j.bolt.testing.assertions.ResponseMessageAssertions;
import org.neo4j.bolt.testing.assertions.SuccessMessageAssertions;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.Level;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

class NetworkResponseHandlerTest {

    private EmbeddedChannel channel;

    private Connection connection;
    private MetadataHandler metadataHandler;

    private AssertableLogProvider internalLog;
    private LogService logService;

    @BeforeEach
    void prepare() {
        this.channel = new EmbeddedChannel();

        this.connection =
                ConnectionMockFactory.newFactory().withChannel(this.channel).build();

        this.metadataHandler = Mockito.mock(MetadataHandler.class);

        this.internalLog = new AssertableLogProvider();
        this.logService = new SimpleLogService(NullLogProvider.getInstance(), this.internalLog);
    }

    @Test
    void shouldPrepareRecordHandler() {
        var handler = new NetworkResponseHandler(this.connection, this.metadataHandler, 512, 0, this.logService);

        var recordHandler = handler.onBeginStreaming(List.of("foo", "bar"));

        Assertions.assertThat(recordHandler).isNotNull().isInstanceOf(NetworkRecordHandler.class);
    }

    @Test
    void shouldAssembleSuccessResponse() {
        var handler = new NetworkResponseHandler(this.connection, this.metadataHandler, 512, 0, this.logService);

        handler.onMetadata("foo", Values.stringValue("bar"));
        handler.onMetadata("baz", Values.stringValue("foo"));

        handler.onSuccess();

        var response = this.channel.<SuccessMessage>readOutbound();

        Assertions.assertThat(response)
                .isNotNull()
                .asInstanceOf(SuccessMessageAssertions.successMessage())
                .hasMeta(meta -> MapValueAssertions.assertThat(meta)
                        .hasSize(2)
                        .containsEntry("foo", Values.stringValue("bar"))
                        .containsEntry("baz", Values.stringValue("foo")));
    }

    @Test
    void shouldUseEmptyMapValueInSuccessResponseWhenNoMetadataIsGiven() {
        var handler = new NetworkResponseHandler(this.connection, this.metadataHandler, 512, 0, this.logService);

        handler.onSuccess();

        var response = this.channel.<SuccessMessage>readOutbound();

        Assertions.assertThat(response)
                .isNotNull()
                .asInstanceOf(SuccessMessageAssertions.successMessage())
                .hasMeta(meta -> MapValueAssertions.assertThat(meta).isSameAs(MapValue.EMPTY));
    }

    @Test
    void shouldAssembleIgnoredResponse() {
        var handler = new NetworkResponseHandler(this.connection, this.metadataHandler, 512, 0, this.logService);

        handler.onIgnored();

        var response = this.channel.<IgnoredMessage>readOutbound();

        Assertions.assertThat(response)
                .asInstanceOf(ResponseMessageAssertions.responseMessage())
                .isIgnoredResponse()
                .isSameAs(IgnoredMessage.INSTANCE);
    }

    @Test
    void shouldAssembleFailureResponse() {
        var handler = new NetworkResponseHandler(this.connection, this.metadataHandler, 512, 0, this.logService);

        handler.onFailure(Error.from(Status.Transaction.Terminated, "Something went wrong!"));

        var response = this.channel.<FailureMessage>readOutbound();

        FailureMessageAssertions.assertThat(response)
                .hasStatus(Status.Transaction.Terminated)
                .hasMessage("Something went wrong!")
                .isNotFatal();
    }

    @Test
    void shouldAssembleFatalFailureResponse() {
        var handler = new NetworkResponseHandler(this.connection, this.metadataHandler, 512, 0, this.logService);

        handler.onFailure(Error.fatalFrom(Status.Transaction.Terminated, "Something went wrong!"));

        var response = this.channel.<FailureMessage>readOutbound();

        FailureMessageAssertions.assertThat(response)
                .hasStatus(Status.Transaction.Terminated)
                .hasMessage("Something went wrong!")
                .isFatal();

        LogAssertions.assertThat(this.internalLog)
                .forLevel(Level.DEBUG)
                .forClass(NetworkResponseHandler.class)
                .containsMessages("Publishing fatal error");
    }
}
