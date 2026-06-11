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
package org.neo4j.server.queryapi.driver.boltmessage.codec;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.connection.netty.impl.messaging.Message;
import org.neo4j.boltmessages.response.FailureMessage;
import org.neo4j.boltmessages.response.FailureMetadata;
import org.neo4j.boltmessages.response.IgnoredMessage;
import org.neo4j.boltmessages.response.RecordMessage;
import org.neo4j.boltmessages.response.ResponseMessage;
import org.neo4j.boltmessages.response.SuccessMessage;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.queryapi.driver.boltmessage.pipeline.InboundMessageDecoder;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;

public class InboundMessageDecoderTest {

    static Stream<Arguments> values() {
        return Stream.of(
                Arguments.of(
                        new SuccessMessage(MapValue.EMPTY),
                        new org.neo4j.bolt.connection.netty.impl.messaging.response.SuccessMessage(Map.of())),
                Arguments.of(
                        new FailureMessage(
                                new FailureMetadata(
                                        Status.General.UnknownError,
                                        "oopsies",
                                        "old oopsies",
                                        "something terrible has happened",
                                        "terrible.GQL",
                                        Map.of(),
                                        null),
                                true),
                        new GQLFailureMessage(new FailureMessage(
                                new FailureMetadata(
                                        Status.General.UnknownError,
                                        "oopsies",
                                        "old oopsies",
                                        "something terrible has happened",
                                        "terrible.GQL",
                                        Map.of(),
                                        null),
                                true))),
                Arguments.of(
                        new RecordMessage(ListValueBuilder.newListBuilder()
                                .add(Values.stringValue("42"))
                                .build()),
                        new org.neo4j.bolt.connection.netty.impl.messaging.response.RecordMessage(
                                List.of(new StringValue("42")))),
                Arguments.of(
                        IgnoredMessage.INSTANCE,
                        org.neo4j.bolt.connection.netty.impl.messaging.response.IgnoredMessage.IGNORED));
    }

    @ParameterizedTest
    @MethodSource("values")
    void encode(ResponseMessage responseMessage, Message expectedDriverMessage) {
        assertThat(InboundMessageDecoder.decode(responseMessage))
                .usingRecursiveComparison()
                .isEqualTo(expectedDriverMessage);
    }
}
