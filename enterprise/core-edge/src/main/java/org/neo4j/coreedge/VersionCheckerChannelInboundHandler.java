/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Predicate;

import org.neo4j.coreedge.messaging.Message;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public abstract class VersionCheckerChannelInboundHandler<M extends Message> extends SimpleChannelInboundHandler<M>
{
    private final Predicate<Message> versionChecker;
    private final Log log;

    protected VersionCheckerChannelInboundHandler( Predicate<Message> versionChecker, LogProvider logProvider )
    {
        this.versionChecker = versionChecker;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected final void channelRead0( ChannelHandlerContext ctx, M message ) throws Exception
    {
        if ( !versionChecker.test( message ) )
        {
            log.error( "Unsupported version %d, unable to process message %s", message.version(), message );
            return;
        }

        doChannelRead0( ctx, message );
    }

    protected abstract void doChannelRead0( ChannelHandlerContext ctx, M msg ) throws Exception;
}
