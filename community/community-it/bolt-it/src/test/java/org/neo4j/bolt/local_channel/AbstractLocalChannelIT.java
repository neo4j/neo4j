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

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.testing.client.UnwiredTestConnection;
import org.neo4j.boltmessages.request.authentication.HelloMessage;
import org.neo4j.boltmessages.request.authentication.LogonMessage;
import org.neo4j.boltmessages.request.connection.RoutingContext;
import org.neo4j.boltmessages.response.FailureMessage;
import org.neo4j.boltmessages.response.RecordMessage;
import org.neo4j.boltmessages.response.ResponseMessage;
import org.neo4j.boltmessages.response.SuccessMessage;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

public class AbstractLocalChannelIT {

    @SafeVarargs
    protected static void assertSuccess(ResponseMessage message, Consumer<SuccessMessage>... assertions) {
        Assertions.assertInstanceOf(SuccessMessage.class, message);
        SuccessMessage successMessage = (SuccessMessage) message;
        for (Consumer<SuccessMessage> assertion : assertions) {
            assertion.accept(successMessage);
        }
    }

    @SafeVarargs
    protected static Consumer<SuccessMessage> assertSuccessHasFields(String... fields) {
        return AbstractLocalChannelIT.assertSuccessField("fields", anyValue -> {
            Assertions.assertInstanceOf(ListValue.class, anyValue);
            var fieldsList = (ListValue) anyValue;
            Assertions.assertEquals(fields.length, fieldsList.intSize());
            for (var i = 0; i < fields.length; i++) {
                Assertions.assertInstanceOf(StringValue.class, fieldsList.value(i));
                var field = ((StringValue) fieldsList.value(i)).stringValue();
                Assertions.assertEquals(fields[i], field);
            }
        });
    }

    protected static Consumer<SuccessMessage> assertSuccessHasTFirst() {
        return AbstractLocalChannelIT.assertSuccessField(
                "t_first", anyValue -> Assertions.assertInstanceOf(LongValue.class, anyValue));
    }

    protected static Consumer<SuccessMessage> assertSuccessHasTLast() {
        return AbstractLocalChannelIT.assertSuccessField(
                "t_last", anyValue -> Assertions.assertInstanceOf(LongValue.class, anyValue));
    }

    protected static Consumer<SuccessMessage> assertSuccessDb(String database) {
        return AbstractLocalChannelIT.assertSuccessField("db", anyValue -> {
            Assertions.assertInstanceOf(StringValue.class, anyValue);
            Assertions.assertEquals(database, ((StringValue) anyValue).stringValue());
        });
    }

    protected static Consumer<SuccessMessage> assertSuccessType(String type) {
        return AbstractLocalChannelIT.assertSuccessField("type", anyValue -> {
            Assertions.assertInstanceOf(StringValue.class, anyValue);
            Assertions.assertEquals(type, ((StringValue) anyValue).stringValue());
        });
    }

    protected static Consumer<SuccessMessage> assertSuccessHasBookmark() {
        return AbstractLocalChannelIT.assertSuccessField("bookmark", anyValue -> {
            Assertions.assertInstanceOf(StringValue.class, anyValue);
            Assertions.assertFalse(((StringValue) anyValue).stringValue().isEmpty());
        });
    }

    @SafeVarargs
    protected static Consumer<SuccessMessage> assertSuccessHasStatuses(Map<String, AnyValue>... statuses) {
        return AbstractLocalChannelIT.assertSuccessField("statuses", anyValue -> {
            Assertions.assertInstanceOf(ListValue.class, anyValue);
            var statusesList = (ListValue) anyValue;
            Assertions.assertEquals(statuses.length, statusesList.intSize());
            for (var i = 0; i < statuses.length; i++) {
                Assertions.assertInstanceOf(MapValue.class, statusesList.value(i));
                var actual = ((MapValue) statusesList.value(i));
                for (var key : actual.keySet()) {
                    Assertions.assertEquals(
                            statuses[0].get(key),
                            actual.get(key),
                            String.format("statuses[%d][%s] is not equal to actual[%d][%s]", i, key, i, key));
                }
            }
        });
    }

    protected static Consumer<SuccessMessage> assertSuccessEmpty() {
        return successMessage -> {
            Assertions.assertTrue(successMessage.metadata().isEmpty());
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
            Assertions.assertTrue(
                    successMessage.metadata().containsKey(field),
                    () -> String.format("Metadata should contain field %s but it doesn't", field));
            for (Consumer<AnyValue> assertion : assertions) {
                assertion.accept(successMessage.metadata().get(field));
            }
        };
    }

    protected static void assertRecord(ResponseMessage firstRecord, AnyValue... expectedValues) {
        Function<AnyValue, BiConsumer<Integer, AnyValue>> assertion = expectedValue ->
                (i, actual) -> Assertions.assertEquals(expectedValue, actual, String.format("Value at index %d", i));

        var assertions = Stream.of(expectedValues).map(assertion).toArray(BiConsumer[]::new);

        AbstractLocalChannelIT.assertRecord(firstRecord, assertions);
    }

    @SafeVarargs
    protected static void assertRecord(
            ResponseMessage firstRecord, BiConsumer<Integer, AnyValue>... expectedValueAssertion) {
        Assertions.assertInstanceOf(RecordMessage.class, firstRecord);
        RecordMessage recordMessage = (RecordMessage) firstRecord;
        Assertions.assertEquals(expectedValueAssertion.length, recordMessage.values.intSize());
        for (int i = 0; i < expectedValueAssertion.length; i++) {
            expectedValueAssertion[i].accept(i, recordMessage.values.value(i));
        }
    }

    protected static Map<String, AnyValue> status(String status, String statusDescription) {
        return Map.of(
                "gql_status", Values.stringValue(status),
                "status_description", Values.stringValue(statusDescription));
    }

    protected static void login(UnwiredTestConnection unwired) {
        unwired.sendRequest(new HelloMessage("test/embedded", List.of(), new RoutingContext(false, Map.of()), null))
                .sendRequest(new LogonMessage(Map.of("scheme", "none")));

        AbstractLocalChannelIT.assertSuccess(
                unwired.receiveResponse(),
                AbstractLocalChannelIT.assertSuccessFields("server", "connection_id", "hints"));
        AbstractLocalChannelIT.assertSuccess(unwired.receiveResponse(), AbstractLocalChannelIT.assertSuccessEmpty());
    }

    protected static void assertFailure(ResponseMessage response) {
        Assertions.assertInstanceOf(FailureMessage.class, response);
    }

    @SettingsFunction
    protected void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(BoltConnectorInternalSettings.enable_object_messages_local_connector, true);
    }
}
