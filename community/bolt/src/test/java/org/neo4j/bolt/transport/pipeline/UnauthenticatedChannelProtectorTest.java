/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.transport.pipeline;

import io.netty.channel.ChannelPipeline;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class UnauthenticatedChannelProtectorTest
{
    @Test
    void shouldInstallAuthenticationHandlersAfterChannelCreated()
    {
        var pipeline = mock( ChannelPipeline.class );
        var protector = new UnauthenticatedChannelProtector( pipeline, Duration.ZERO, -1 );

        InOrder inOrder = inOrder( pipeline );
        protector.afterChannelCreated();
        inOrder.verify( pipeline ).addLast( any( AuthenticationTimeoutTracker.class ) );
        inOrder.verify( pipeline ).addLast( any( AuthenticationTimeoutHandler.class ) );
    }

    @Test
    void shouldInstallByteAccumulatorBeforeBoltProtocolInstalled()
    {
        var pipeline = mock( ChannelPipeline.class );
        var protector = new UnauthenticatedChannelProtector( pipeline, Duration.ZERO, 0 );

        protector.beforeBoltProtocolInstalled();
        verify( pipeline ).addLast( any( BytesAccumulator.class ) );
        verifyNoMoreInteractions( pipeline );
    }

    @Test
    void shouldRemoveHandlersWhenIsDisabled()
    {
        var pipeline = mock( ChannelPipeline.class );
        var protector = new UnauthenticatedChannelProtector( pipeline, Duration.ZERO, 0 );

        InOrder inOrder = inOrder( pipeline );
        protector.disable();
        inOrder.verify( pipeline ).remove( AuthenticationTimeoutTracker.class );
        inOrder.verify( pipeline ).remove( AuthenticationTimeoutHandler.class );
        inOrder.verify( pipeline ).remove( BytesAccumulator.class );
    }
}
