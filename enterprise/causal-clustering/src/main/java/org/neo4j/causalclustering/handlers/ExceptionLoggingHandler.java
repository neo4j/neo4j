/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.handlers;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import org.neo4j.logging.Log;

import static java.lang.String.format;

public class ExceptionLoggingHandler extends ChannelHandlerAdapter
{
    private final Log log;

    public ExceptionLoggingHandler( Log log )
    {
        this.log = log;
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
    {
        log.error( message( ctx ), cause );
        ctx.fireExceptionCaught( cause );
    }

    private String message( ChannelHandlerContext ctx )
    {
        return ctx != null ? format( "Failed to process message on channel %s.", ctx.channel() )
                           : "Failed to process message on a null channel.";
    }
}
