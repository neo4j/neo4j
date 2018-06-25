/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.transport;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;

import org.neo4j.bolt.logging.BoltMessageLogging;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class TransportSelectionHandlerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @After
    public void cleanup()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldHandleExceptions()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        RuntimeException error = new RuntimeException();

        channel.pipeline().addLast( newHandler( logProvider ) );

        channel.pipeline().fireExceptionCaught( error );

        assertFalse( channel.isActive() );
        logProvider.assertAtLeastOnce( inLog( TransportSelectionHandler.class ).error(
                startsWith( "Fatal error occurred during protocol selection" ),
                equalTo( error ) ) );
    }

    private static TransportSelectionHandler newHandler( AssertableLogProvider logProvider )
    {
        return new TransportSelectionHandler( "bolt", null, false, false,
                logProvider, mock( BoltProtocolPipelineInstallerFactory.class ), BoltMessageLogging.none() );
    }
}
