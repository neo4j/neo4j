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
package org.neo4j.server.queryapi.driver.boltmessage.pipeline;

import static java.util.Objects.requireNonNull;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.bolt.connection.GqlError;
import org.neo4j.bolt.connection.netty.impl.async.connection.ChannelAttributes;
import org.neo4j.bolt.connection.netty.impl.async.inbound.InboundMessageDispatcher;
import org.neo4j.bolt.connection.netty.impl.messaging.response.IgnoredMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.RecordMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.SuccessMessage;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.boltmessages.response.FailureMessage;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.server.queryapi.driver.boltmessage.codec.GQLFailureMessage;

public class InboundMessageReader extends ChannelInboundHandlerAdapter {

    private InboundMessageDispatcher messageDispatcher;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        messageDispatcher = requireNonNull(ChannelAttributes.messageDispatcher(ctx.channel()));
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        messageDispatcher = null;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        switch (msg) {
            case SuccessMessage successMessage -> messageDispatcher.handleSuccessMessage(successMessage.metadata());
            case RecordMessage recordMessage -> messageDispatcher.handleRecordMessage(recordMessage.fields());
            case IgnoredMessage ignoredMessage -> messageDispatcher.handleIgnoredMessage();
            case GQLFailureMessage failureMessage ->
                messageDispatcher.handleFailureMessage(toGqlError(failureMessage.failureMessage()));
            default -> {}
        }
    }

    private GqlError toGqlError(FailureMessage gqlFailureMessage) {
        return new GqlError(
                gqlFailureMessage.status().code().serialize(),
                gqlFailureMessage.message(),
                gqlFailureMessage.status().code().serialize(),
                gqlFailureMessage.metadata().message(),
                convertDiagnosticRecord(gqlFailureMessage.metadata().diagnosticRecord()),
                null);
    }

    @SuppressWarnings("rawtypes")
    private Map<String, Value> convertDiagnosticRecord(Map<String, Object> diagnosticRecord) {
        var convertedDiagnosticRecord = new HashMap<String, Value>();
        diagnosticRecord.forEach((key, value) -> {
            switch (value) {
                case String string -> convertedDiagnosticRecord.put(key, new StringValue(string));
                case Integer integer -> convertedDiagnosticRecord.put(key, new IntegerValue(integer));
                case Map map -> convertedDiagnosticRecord.put(key, new MapValue(convertDiagnosticRecord(map)));
                default ->
                    throw new UnsupportedOperationException(
                            "Unsupported diagnostic record value type: " + value.getClass());
            }
        });
        return convertedDiagnosticRecord;
    }
}
