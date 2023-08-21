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
package org.neo4j.bolt.protocol.common.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.packstream.error.reader.PackstreamReaderException;

/**
 * Enqueues requests and error handling logic on the connection state machine.
 * <p>
 * This handler only considers Packstream and status bearing errors within its error handling logic. Any remaining exceptions will be forwarded to the following
 * handlers as-is and will thus be considered protocol errors which require connection termination.
 */
public class RequestHandler extends SimpleChannelInboundHandler<RequestMessage> {
    private final InternalLog log;

    private Connection connection;

    public RequestHandler(InternalLogProvider logging) {
        this.log = logging.getLog(RequestHandler.class);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.connection = Connection.getConnection(ctx.channel());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RequestMessage msg) throws Exception {
        log.debug("Submitting job for message %s", msg);
        connection.submit(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        var ex = cause;

        // unpack DecoderException if possible as netty will use this type to wrap any exceptions thrown within a
        // Decoder
        // implementation thus hiding important argument related errors from us
        if (cause instanceof DecoderException) {
            var inner = cause.getCause();
            if (inner != null) {
                ex = inner;
            }
        }

        // filter any unknown exception types and pass them to following handlers within the pipeline as we cannot
        // generate
        // a valid response otherwise.
        if (!(ex instanceof PackstreamReaderException) && !(ex instanceof Status.HasStatus)) {
            // re-fire with original parameters instead of unpacked exception as we do not wish to remove any
            // information
            // that other handlers may wish to retain (such as HouseKeeperHandler)
            super.exceptionCaught(ctx, cause);
            return;
        }

        // all status bearing errors are enqueued on the state machine for reporting (e.g. as a FAILURE message)
        var error = Error.from(cause);
        connection.submit((fsm, responseHandler) -> responseHandler.onFailure(error));
    }
}
