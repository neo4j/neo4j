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
package org.neo4j.causalclustering.core.server;

import io.netty.channel.EventLoopGroup;

import java.util.concurrent.TimeUnit;

public class ServerShutdown
{
    private static final int SHUTDOWN_TIMEOUT = 170;
    private static final int TIMEOUT = 180;
    private static final int QUIET_PERIOD = 2;
    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    public static void shutdown( EventLoopGroup eventLoopGroup ) throws Exception
    {
        if ( eventLoopGroup != null )
        {
            eventLoopGroup.shutdownGracefully( QUIET_PERIOD, SHUTDOWN_TIMEOUT, TIME_UNIT ).get( TIMEOUT, TIME_UNIT );
        }
    }
}
