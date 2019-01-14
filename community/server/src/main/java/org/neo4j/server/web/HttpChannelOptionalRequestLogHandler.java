/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.web;

import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.RequestLogHandler;

import java.io.IOException;
import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.neo4j.server.rest.web.InternalJettyServletRequest;

/**
 * This is the log handler used for http logging.
 * This class overrides the original {@link RequestLogHandler}
 * and rewrite the {@link RequestLogHandler#handle(String, Request, HttpServletRequest, HttpServletResponse)}
 * to be able to accept {@link Request} who does not have a http channel attached with it, such as {@link InternalJettyServletRequest}.
 */
public class HttpChannelOptionalRequestLogHandler extends RequestLogHandler
{
    @Override
    public void handle( String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response ) throws IOException, ServletException
    {
        HttpChannel httpChannel = baseRequest.getHttpChannel();
        if ( httpChannel != null ) // if the channel is not null, all good, you handle yourself.
        {
            super.handle( target, baseRequest, request, response );
        }
        else // if we do not have a real channel, then we just log ourselves
        {
            try
            {
                if ( _handler != null )
                {
                    _handler.handle( target, baseRequest, request, response );
                }
            }
            finally
            {
                RequestLog requestLog = getRequestLog();
                if ( requestLog != null && baseRequest.getDispatcherType() == DispatcherType.REQUEST )
                {
                    requestLog.log( baseRequest, (Response) response );
                }
            }
        }
    }
}
