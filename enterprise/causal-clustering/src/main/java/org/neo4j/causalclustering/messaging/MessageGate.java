/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.neo4j.causalclustering.protocol.handshake.GateEvent;

/**
 * Gates messages and keeps them on a queue until the gate is either
 * opened successfully or closed forever.
 */
@ChannelHandler.Sharable
public class MessageGate extends ChannelDuplexHandler
{
    private final Predicate<Object> gated;

    private List<GatedWrite> pending = new ArrayList<>();

    public MessageGate( Predicate<Object> gated )
    {
        this.gated = gated;
    }

    @Override
    public void userEventTriggered( ChannelHandlerContext ctx, Object evt ) throws Exception
    {
        if ( evt instanceof GateEvent )
        {
            if ( GateEvent.getSuccess().equals( evt ) )
            {
                for ( GatedWrite write : pending )
                {
                    ctx.write( write.msg, write.promise );
                }

                ctx.channel().pipeline().remove( this );
            }

            pending.clear();
            pending = null;
        }
        else
        {
            super.userEventTriggered( ctx, evt );
        }
    }

    @Override
    public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
    {
        if ( !gated.test( msg ) )
        {
            ctx.write( msg, promise );
        }
        else if ( pending != null )
        {
            pending.add( new GatedWrite( msg, promise ) );
        }
        else
        {
            promise.setFailure( new RuntimeException( "Gate failed and has been permanently closed." ) );
        }
    }

    static class GatedWrite
    {
        final Object msg;
        final ChannelPromise promise;

        GatedWrite( Object msg, ChannelPromise promise )
        {
            this.msg = msg;
            this.promise = promise;
        }
    }
}
