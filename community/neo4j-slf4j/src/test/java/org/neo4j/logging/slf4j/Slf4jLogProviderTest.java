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
package org.neo4j.logging.slf4j;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import org.neo4j.logging.Log;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;

class Slf4jLogProviderTest
{
    private final Slf4jLogProvider logProvider = new Slf4jLogProvider();

    @BeforeEach
    void clearLoggingEventsAccumulator()
    {
        getAccumulatingAppender().clearEventsList();
    }

    @Test
    void shouldLogDebug()
    {
        Log log = logProvider.getLog( getClass() );

        log.debug( "Holy debug batman!" );

        assertLogOccurred( Level.DEBUG, "Holy debug batman!" );
    }

    @Test
    void shouldLogInfo()
    {
        Log log = logProvider.getLog( getClass() );

        log.info( "Holy info batman!" );

        assertLogOccurred( Level.INFO, "Holy info batman!" );
    }

    @Test
    void shouldLogWarning()
    {
        Log log = logProvider.getLog( getClass() );

        log.warn( "Holy warning batman!" );

        assertLogOccurred( Level.WARN, "Holy warning batman!" );
    }

    @Test
    void shouldLogError()
    {
        Log log = logProvider.getLog( getClass() );

        log.error( "Holy error batman!" );

        assertLogOccurred( Level.ERROR, "Holy error batman!" );
    }

    private void assertLogOccurred( Level level, String message )
    {
        ArrayList<LoggingEvent> events = getLoggingEvents();
        assertThat( events, hasSize( 1 ) );
        LoggingEvent event = events.get( 0 );
        assertThat( event.getLoggerName(), is( getClass().getName() ) );
        assertThat( event.getLevel(), is( level ) );
        assertThat( event.getMessage(), is( message ) );
    }

    private static ArrayList<LoggingEvent> getLoggingEvents()
    {
        return getAccumulatingAppender().getEventsList();
    }

    private static AccumulatingAppender getAccumulatingAppender()
    {
        return (AccumulatingAppender) Logger.getRootLogger().getAppender( "accumulating" );
    }
}
