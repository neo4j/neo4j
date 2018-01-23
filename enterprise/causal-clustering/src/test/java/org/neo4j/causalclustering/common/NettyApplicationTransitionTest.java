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
package org.neo4j.causalclustering.common;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@RunWith( Parameterized.class )
public class NettyApplicationTransitionTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    @Rule
    public final NettyApplicationHelper nettyApplicationHelper = new NettyApplicationHelper();

    @Parameterized.Parameters( name = "from {0} to {1} is allowed? {2}" )
    public static Iterable<Object[]> transitions()
    {
        ArrayList<Object[]> transitions = new ArrayList<>();
        for ( LifeCycleState from : LifeCycleState.values() )
        {
            for ( LifeCycleState to : LifeCycleState.values() )
            {
                transitions.add( new Object[]{from, to, isAllowedTransition( from, to )} );
            }
        }
        return transitions;
    }

    @Parameterized.Parameter()
    public LifeCycleState from;

    @Parameterized.Parameter( 1 )
    public LifeCycleState to;

    @Parameterized.Parameter( 2 )
    public boolean isAllowed;

    @Test
    public void shouldAllowOrRejectTransition() throws Throwable
    {
        if ( !isAllowed )
        {
            expectedException.expect( IllegalStateException.class );
        }
        EventLoopGroup executor = nettyApplicationHelper.createMockedEventExecutor( doReturn( null ) );
        EventLoopContext<ServerChannel> context = new EventLoopContext<>( executor, ServerChannel.class );
        ChannelService<ServerBootstrap,ServerChannel> channelService = mock( ChannelService.class );
        NettyApplication<ServerChannel> nettyApplication = new NettyApplication<>( channelService, () -> context );
        from.gentlyTransitionToMyState( nettyApplication );
        to.setMyState( nettyApplication );
    }

    private enum LifeCycleState
    {
        INIT
                {
                    @Override
                    boolean isAllowedTransition( LifeCycleState toState )
                    {
                        return toState == START || toState == STOP || toState == SHUTDOWN;
                    }

                    @Override
                    void gentlyTransitionToMyState( NettyApplication nettyApplication ) throws Throwable
                    {
                        nettyApplication.init();
                    }

                    @Override
                    void setMyState( NettyApplication nettyApplication ) throws Throwable
                    {
                        nettyApplication.init();
                    }
                },
        START
                {
                    @Override
                    boolean isAllowedTransition( LifeCycleState toState )
                    {
                        return toState == START || toState == STOP || toState == SHUTDOWN;
                    }

                    @Override
                    void gentlyTransitionToMyState( NettyApplication nettyApplication ) throws Throwable
                    {
                        nettyApplication.init();
                        nettyApplication.start();
                    }

                    @Override
                    void setMyState( NettyApplication nettyApplication ) throws Throwable
                    {
                        nettyApplication.start();
                    }
                },
        STOP
                {
                    @Override
                    boolean isAllowedTransition( LifeCycleState toState )
                    {
                        return toState == START || toState == STOP || toState == SHUTDOWN;
                    }

                    @Override
                    void gentlyTransitionToMyState( NettyApplication nettyApplication ) throws Throwable
                    {
                        nettyApplication.init();
                        nettyApplication.start();
                        nettyApplication.stop();
                    }

                    @Override
                    void setMyState( NettyApplication nettyApplication ) throws Throwable
                    {
                        nettyApplication.stop();
                    }
                },
        SHUTDOWN
                {
                    @Override
                    boolean isAllowedTransition( LifeCycleState toState )
                    {
                        return toState == SHUTDOWN || toState == STOP;
                    }

                    @Override
                    void gentlyTransitionToMyState( NettyApplication nettyApplication ) throws Throwable
                    {
                        nettyApplication.init();
                        nettyApplication.start();
                        nettyApplication.stop();
                        nettyApplication.shutdown();
                    }

                    @Override
                    void setMyState( NettyApplication nettyApplication ) throws Throwable
                    {
                        nettyApplication.shutdown();
                    }
                };

        abstract boolean isAllowedTransition( LifeCycleState toState );

        abstract void gentlyTransitionToMyState( NettyApplication nettyApplication ) throws Throwable;

        abstract void setMyState( NettyApplication nettyApplication ) throws Throwable;
    }

    private static boolean isAllowedTransition( LifeCycleState from, LifeCycleState to )
    {
        return from.isAllowedTransition( to );
    }
}
