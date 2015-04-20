/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ext.monitorlogging;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.neo4j.kernel.impl.util.TestLogging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.impl.util.TestLogger.LogCall.debug;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.error;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.info;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.warn;

public class LoggingListenerTest
{

    interface InterestingMonitor1
    {
        void touch();

        void touch( String argument );
    }

    interface InterestingMonitor2
    {
        void touch();

        void doubleTouch( String someString, int someInt );
    }

    @Test
    public void shouldLogWhenThingsAreMonitored()
    {
        // Given
        Monitors monitors = new Monitors();
        TestLogging logging = new TestLogging();
        InterestingMonitor1 aMonitor = monitors.newMonitor( InterestingMonitor1.class );
        LoggingListener listener = new LoggingListener( logging,
                Collections.<Class<?>, LogLevel>singletonMap( InterestingMonitor1.class, LogLevel.INFO )
        );
        monitors.addMonitorListener( listener, listener.predicate );

        // When
        aMonitor.touch();

        // Then
        logging.getMessagesLog( InterestingMonitor1.class ).assertExactly( info( "touch()" ) );
    }

    @Test
    public void shouldNotLogWhenThingsAreNotRegisteredInTheMonitorListener()
    {
        // Given
        Monitors monitors = new Monitors();
        TestLogging logging = new TestLogging();
        InterestingMonitor1 aMonitor = monitors.newMonitor( InterestingMonitor1.class );
        LoggingListener listener = new LoggingListener( logging, Collections.<Class<?>, LogLevel>emptyMap() );
        monitors.addMonitorListener( listener, listener.predicate );

        // When
        aMonitor.touch();

        // Then
        logging.getMessagesLog( InterestingMonitor1.class ).assertNoInfos();
    }

    @Test
    public void shouldBeAbleToListenToMultipleMonitors()
    {
        // Given
        Monitors monitors = new Monitors();
        TestLogging logging = new TestLogging();
        InterestingMonitor1 aMonitor = monitors.newMonitor( InterestingMonitor1.class );
        InterestingMonitor2 bMonitor = monitors.newMonitor( InterestingMonitor2.class );
        Map<Class<?>, LogLevel> classes = new HashMap<>( 2 );
        classes.put( InterestingMonitor1.class, LogLevel.DEBUG );
        classes.put( InterestingMonitor2.class, LogLevel.WARN );
        LoggingListener listener = new LoggingListener( logging, classes );
        monitors.addMonitorListener( listener, listener.predicate );

        // When
        aMonitor.touch();
        bMonitor.touch();

        // Then
        logging.getMessagesLog( InterestingMonitor1.class ).assertExactly( debug( "touch()" ) );
        logging.getMessagesLog( InterestingMonitor2.class ).assertExactly( warn( "touch()" ) );
    }

    @Test
    public void shouldBeAbleToOutputAMethodArgument()
    {
        // Given
        Monitors monitors = new Monitors();
        TestLogging logging = new TestLogging();
        InterestingMonitor1 aMonitor = monitors.newMonitor( InterestingMonitor1.class );
        LoggingListener listener = new LoggingListener( logging,
                Collections.<Class<?>, LogLevel>singletonMap( InterestingMonitor1.class, LogLevel.ERROR )
        );
        monitors.addMonitorListener( listener, listener.predicate );

        // When
        aMonitor.touch( "APA" );

        // Then
        logging.getMessagesLog( InterestingMonitor1.class ).assertExactly( error( "touch(String:APA)" ) );
    }

    @Test
    public void shouldBeAbleToOutputMultipleMethodArguments()
    {
        // Given
        Monitors monitors = new Monitors();
        TestLogging logging = new TestLogging();
        InterestingMonitor2 aMonitor = monitors.newMonitor( InterestingMonitor2.class );
        LoggingListener listener = new LoggingListener( logging,
                Collections.<Class<?>, LogLevel>singletonMap( InterestingMonitor2.class, LogLevel.WARN )
        );
        monitors.addMonitorListener( listener, listener.predicate );

        // When
        aMonitor.doubleTouch( "APA", 42 );

        // Then
        logging.getMessagesLog( InterestingMonitor2.class ).assertExactly( warn( "doubleTouch(String:APA,int:42)" ) );
    }
}
