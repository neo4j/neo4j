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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.neo4j.bolt.connection.netty.impl.messaging.Message;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.boltmessages.response.FailureMessage;
import org.neo4j.boltmessages.response.IgnoredMessage;
import org.neo4j.boltmessages.response.RecordMessage;
import org.neo4j.boltmessages.response.ResponseMessage;
import org.neo4j.boltmessages.response.SuccessMessage;
import org.neo4j.server.queryapi.driver.boltmessage.codec.BoltMessageValueDecoder;
import org.neo4j.server.queryapi.driver.boltmessage.codec.GQLFailureMessage;

public class InboundMessageDecoder extends MessageToMessageDecoder<ResponseMessage> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ResponseMessage msg, List<Object> out) throws Exception {
        out.add(decode(msg));
    }

    public static Message decode(ResponseMessage responseMessage) {
        return switch (responseMessage) {
            case SuccessMessage successMessage -> {
                var metaMap = new HashMap<String, Value>();
                successMessage
                        .metadata()
                        .foreach((key, value) -> metaMap.put(key, BoltMessageValueDecoder.decode(value)));
                yield new org.neo4j.bolt.connection.netty.impl.messaging.response.SuccessMessage(metaMap);
            }
            case FailureMessage failureMessage ->
                // TODO consider pushing this to the bolt-api
                new GQLFailureMessage(failureMessage);
            case RecordMessage recordMessage -> {
                var recordList = new ArrayList<Value>();
                recordMessage.values.forEach(value -> recordList.add(BoltMessageValueDecoder.decode(value)));
                yield new org.neo4j.bolt.connection.netty.impl.messaging.response.RecordMessage(recordList);
            }
            case IgnoredMessage ignoredMessage ->
                org.neo4j.bolt.connection.netty.impl.messaging.response.IgnoredMessage.IGNORED;
            default -> throw new IllegalStateException("Unexpected response message: " + responseMessage);
        };
    }
}
