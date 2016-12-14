/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.test.ThreadTestUtils;

public abstract class NeoServerRestartTest
{

    @Test
    public void shouldNotCorruptWhenImmediatelyStopped() throws IOException, InterruptedException
    {
        NeoServer server = getNeoServer();

        CountDownLatch latch = new CountDownLatch( 1 );
        ThreadTestUtils.fork( $waitThenStopServer( server, latch ) );
        server.start();
        //Wait for the server to stop.
        latch.await();

        server = getNeoServer();
        server.start();
        server.stop();
    }

    protected abstract NeoServer getNeoServer() throws IOException;

    private Runnable $waitThenStopServer( NeoServer server, CountDownLatch latch )
    {
        return () -> {
            try
            {
                //Make sure that we have started starting the database. This value is empirically chosen and may need
                // to be revisited in the future.
                Thread.sleep( 200 );
                server.stop();
                latch.countDown();
            }
            catch ( Exception e )
            {
                //This is ok, it is not what we are testing for.
            }
        };
    }
}
