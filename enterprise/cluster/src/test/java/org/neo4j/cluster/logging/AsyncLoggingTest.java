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
package org.neo4j.cluster.logging;

import org.junit.Test;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.async.AsyncLogProvider;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class AsyncLoggingTest
{
    @Test
    public void shouldLogMessages() throws Exception
    {
        // given
        AssertableLogProvider logs = new AssertableLogProvider();
        AsyncLogging logging = new AsyncLogging( logs.getLog( "meta" ) );

        // when
        logging.start();
        try
        {
            new AsyncLogProvider( logging.eventSender(), logs ).getLog( "test" ).info( "hello" );
        }
        finally
        {
            logging.stop();
        }
        // then
        logs.assertExactly( inLog( "test" ).info( endsWith( "hello" ) ) );
    }

    @Test
    public void shouldLogWhenLoggingThreadStarts() throws Exception
    {
        // given
        AssertableLogProvider logs = new AssertableLogProvider();
        AsyncLogging logging = new AsyncLogging( logs.getLog( "meta" ) );

        // when
        new AsyncLogProvider( logging.eventSender(), logs ).getLog( "test" ).info( "hello" );

        // then
        logs.assertNoLoggingOccurred();

        // when
        logging.start();
        logging.stop();

        // then
        logs.assertExactly( inLog( "test" ).info( endsWith( "hello" ) ) );
    }
}