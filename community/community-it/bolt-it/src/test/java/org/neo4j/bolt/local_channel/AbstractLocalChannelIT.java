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
package org.neo4j.bolt.local_channel;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.testing.client.UnwiredTestConnection;
import org.neo4j.boltmessages.request.authentication.HelloMessage;
import org.neo4j.boltmessages.request.authentication.LogonMessage;
import org.neo4j.boltmessages.request.connection.RoutingContext;
import org.neo4j.boltmessages.response.FailureMessage;
import org.neo4j.boltmessages.response.RecordMessage;
import org.neo4j.boltmessages.response.ResponseMessage;
import org.neo4j.boltmessages.response.SuccessMessage;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

abstract class AbstractLocalChannelIT {

    @SafeVarargs
    protected static void assertSuccess(ResponseMessage message, Consumer<SuccessMessage>... assertions) {
        assertThat(message).isInstanceOf(SuccessMessage.class);
        SuccessMessage successMessage = (SuccessMessage) message;
        for (Consumer<SuccessMessage> assertion : assertions) {
            assertion.accept(successMessage);
        }
    }

    @SafeVarargs
    protected static Consumer<SuccessMessage> assertSuccessHasFields(String... fields) {
        return AbstractLocalChannelIT.assertSuccessField("fields", anyValue -> {
            assertThat(anyValue).isInstanceOf(ListValue.class);
            var fieldsList = (ListValue) anyValue;
            assertThat(fieldsList.intSize()).isEqualTo(fields.length);
            for (var i = 0; i < fields.length; i++) {
                assertThat(fieldsList.value(i)).isInstanceOf(StringValue.class);
                var field = ((StringValue) fieldsList.value(i)).stringValue();
                assertThat(field).isEqualTo(fields[i]);
            }
        });
    }

    protected static Consumer<SuccessMessage> assertSuccessHasTFirst() {
        return AbstractLocalChannelIT.assertSuccessField(
                "t_first", anyValue -> assertThat(anyValue).isInstanceOf(LongValue.class));
    }

    protected static Consumer<SuccessMessage> assertSuccessHasTLast() {
        return AbstractLocalChannelIT.assertSuccessField(
                "t_last", anyValue -> assertThat(anyValue).isInstanceOf(LongValue.class));
    }

    protected static Consumer<SuccessMessage> assertSuccessDb(String database) {
        return AbstractLocalChannelIT.assertSuccessField("db", anyValue -> {
            assertThat(anyValue).isInstanceOf(StringValue.class);
            assertThat(((StringValue) anyValue).stringValue()).isEqualTo(database);
        });
    }

    protected static Consumer<SuccessMessage> assertSuccessType(String type) {
        return AbstractLocalChannelIT.assertSuccessField("type", anyValue -> {
            assertThat(anyValue).isInstanceOf(StringValue.class);
            assertThat(((StringValue) anyValue).stringValue()).isEqualTo(type);
        });
    }

    protected static Consumer<SuccessMessage> assertSuccessHasBookmark() {
        return AbstractLocalChannelIT.assertSuccessField("bookmark", anyValue -> {
            assertThat(anyValue).isInstanceOf(StringValue.class);
            assertThat(((StringValue) anyValue).stringValue()).isNotEmpty();
        });
    }

    @SafeVarargs
    protected static Consumer<SuccessMessage> assertSuccessHasStatuses(Map<String, AnyValue>... statuses) {
        return AbstractLocalChannelIT.assertSuccessField("statuses", anyValue -> {
            assertThat(anyValue).isInstanceOf(ListValue.class);
            var statusesList = (ListValue) anyValue;
            assertThat(statusesList.intSize()).isEqualTo(statuses.length);
            for (var i = 0; i < statuses.length; i++) {
                assertThat(statusesList.value(i)).isInstanceOf(MapValue.class);
                var actual = ((MapValue) statusesList.value(i));
                for (var key : actual.keySet()) {
                    assertThat(actual.get(key))
                            .as(String.format("statuses[%d][%s] is not equal to actual[%d][%s]", i, key, i, key))
                            .isEqualTo(statuses[0].get(key));
                }
            }
        });
    }

    protected static Consumer<SuccessMessage> assertSuccessEmpty() {
        return successMessage -> {
            assertThat(successMessage.metadata().isEmpty()).isTrue();
        };
    }

    @SafeVarargs
    private static Consumer<SuccessMessage> assertSuccessFields(String... fields) {
        Consumer<SuccessMessage> result = ignored -> {};

        for (String field : fields) {
            result = result.andThen(AbstractLocalChannelIT.assertSuccessField(field));
        }

        return result;
    }

    @SafeVarargs
    private static Consumer<SuccessMessage> assertSuccessField(String field, Consumer<AnyValue>... assertions) {
        return successMessage -> {
            assertThat(successMessage.metadata().containsKey(field))
                    .as(() -> String.format("Metadata should contain field %s but it doesn't", field))
                    .isTrue();
            for (Consumer<AnyValue> assertion : assertions) {
                assertion.accept(successMessage.metadata().get(field));
            }
        };
    }

    protected static void assertRecord(ResponseMessage firstRecord, AnyValue... expectedValues) {
        Function<AnyValue, BiConsumer<Integer, AnyValue>> assertion = expectedValue -> (i, actual) ->
                assertThat(actual).as(String.format("Value at index %d", i)).isEqualTo(expectedValue);

        var assertions = Stream.of(expectedValues).map(assertion).toArray(BiConsumer[]::new);

        AbstractLocalChannelIT.assertRecord(firstRecord, assertions);
    }

    @SafeVarargs
    protected static void assertRecord(
            ResponseMessage firstRecord, BiConsumer<Integer, AnyValue>... expectedValueAssertion) {
        assertThat(firstRecord).isInstanceOf(RecordMessage.class);
        RecordMessage recordMessage = (RecordMessage) firstRecord;
        assertThat(recordMessage.values.intSize()).isEqualTo(expectedValueAssertion.length);
        for (int i = 0; i < expectedValueAssertion.length; i++) {
            expectedValueAssertion[i].accept(i, recordMessage.values.value(i));
        }
    }

    protected static Map<String, AnyValue> status(String status, String statusDescription) {
        return Map.of(
                "gql_status", Values.stringValue(status),
                "status_description", Values.stringValue(statusDescription));
    }

    protected static void authenticate(UnwiredTestConnection unwired) {
        unwired.sendRequest(new HelloMessage("test/embedded", List.of(), new RoutingContext(false, Map.of()), null))
                .sendRequest(new LogonMessage(Map.of("scheme", "none")));

        AbstractLocalChannelIT.assertSuccess(
                unwired.receiveResponse(),
                AbstractLocalChannelIT.assertSuccessFields("server", "connection_id", "hints"));
        AbstractLocalChannelIT.assertSuccess(unwired.receiveResponse(), AbstractLocalChannelIT.assertSuccessEmpty());
    }

    protected static void login(UnwiredTestConnection unwired) {
        unwired.sendRequest(new LogonMessage(Map.of("scheme", "none")));
        AbstractLocalChannelIT.assertSuccess(unwired.receiveResponse(), AbstractLocalChannelIT.assertSuccessEmpty());
    }

    protected static void assertFailure(ResponseMessage response) {
        assertThat(response).isInstanceOf(FailureMessage.class);
    }

    @SettingsFunction
    protected static void customizeSettings(SettingBuilder settings) {
        settings.set(BoltConnectorInternalSettings.enable_object_messages_local_connector, true);
    }
}
