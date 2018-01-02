/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.logging.slf4j;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.logging.Log;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;

public class Slf4jLogProviderTest
{
    Slf4jLogProvider logProvider = new Slf4jLogProvider();

    @Before
    public void clearLoggingEventsAccumulator()
    {
        getAccumulatingAppender().clearEventsList();
    }

    @Test
    public void shouldLogDebug()
    {
        Log log = logProvider.getLog( getClass() );

        log.debug( "Holy debug batman!" );

        assertLogOccurred( Level.DEBUG, "Holy debug batman!" );
    }

    @Test
    public void shouldLogInfo()
    {
        Log log = logProvider.getLog( getClass() );

        log.info( "Holy info batman!" );

        assertLogOccurred( Level.INFO, "Holy info batman!" );
    }

    @Test
    public void shouldLogWarning()
    {
        Log log = logProvider.getLog( getClass() );

        log.warn( "Holy warning batman!" );

        assertLogOccurred( Level.WARN, "Holy warning batman!" );
    }

    @Test
    public void shouldLogError()
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
        assertThat( event.getMessage(), is( (Object) message ) );
    }

    private ArrayList<LoggingEvent> getLoggingEvents()
    {
        return getAccumulatingAppender().getEventsList();
    }

    private AccumulatingAppender getAccumulatingAppender()
    {
        return (AccumulatingAppender) Logger.getRootLogger().getAppender( "accumulating" );
    }
}
