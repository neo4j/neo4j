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
package org.neo4j.ext.monitorlogging;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.logging.AssertableLogProvider.inLog;

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
        AssertableLogProvider logProvider = new AssertableLogProvider();

        InterestingMonitor1 aMonitor = monitors.newMonitor( InterestingMonitor1.class );
        LoggingListener listener = new LoggingListener(
                Collections.<Class<?>, Logger>singletonMap( InterestingMonitor1.class, logProvider.getLog( InterestingMonitor1.class ).infoLogger() )
        );
        monitors.addMonitorListener( listener, listener.predicate );

        // When
        aMonitor.touch();

        // Then
        logProvider.assertExactly(
                inLog( InterestingMonitor1.class ).info( "touch()" )
        );
    }

    @Test
    public void shouldNotLogWhenMatchingClassIsNotRegisteredInTheMonitorListener()
    {
        // Given
        Monitors monitors = new Monitors();
        AssertableLogProvider logProvider = new AssertableLogProvider();

        InterestingMonitor1 aMonitor = monitors.newMonitor( InterestingMonitor1.class );
        LoggingListener listener = new LoggingListener(
                Collections.<Class<?>, Logger>singletonMap( InterestingMonitor2.class, logProvider.getLog( InterestingMonitor2.class ).infoLogger() )
        );
        monitors.addMonitorListener( listener, listener.predicate );

        // When
        aMonitor.touch();

        // Then
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void shouldBeAbleToListenToMultipleMonitors()
    {
        // Given
        Monitors monitors = new Monitors();
        AssertableLogProvider logProvider = new AssertableLogProvider();

        InterestingMonitor1 aMonitor = monitors.newMonitor( InterestingMonitor1.class );
        InterestingMonitor2 bMonitor = monitors.newMonitor( InterestingMonitor2.class );
        Map<Class<?>, Logger> classes = new HashMap<>( 2 );
        classes.put( InterestingMonitor1.class, logProvider.getLog( InterestingMonitor1.class ).debugLogger() );
        classes.put( InterestingMonitor2.class, logProvider.getLog( InterestingMonitor2.class ).warnLogger() );
        LoggingListener listener = new LoggingListener( classes );
        monitors.addMonitorListener( listener, listener.predicate );

        // When
        aMonitor.touch();
        bMonitor.touch();

        // Then
        logProvider.assertExactly(
                inLog( InterestingMonitor1.class ).debug( "touch()" ),
                inLog( InterestingMonitor2.class ).warn( "touch()" )
        );
    }

    @Test
    public void shouldBeAbleToOutputAMethodArgument()
    {
        // Given
        Monitors monitors = new Monitors();
        AssertableLogProvider logProvider = new AssertableLogProvider();

        InterestingMonitor1 aMonitor = monitors.newMonitor( InterestingMonitor1.class );
        LoggingListener listener = new LoggingListener(
                Collections.<Class<?>, Logger>singletonMap( InterestingMonitor1.class, logProvider.getLog( InterestingMonitor1.class ).errorLogger() )
        );
        monitors.addMonitorListener( listener, listener.predicate );

        // When
        aMonitor.touch( "APA" );

        // Then
        logProvider.assertExactly(
                inLog( InterestingMonitor1.class ).error( "touch(String:APA)" )
        );
    }

    @Test
    public void shouldBeAbleToOutputMultipleMethodArguments()
    {
        // Given
        Monitors monitors = new Monitors();
        AssertableLogProvider logProvider = new AssertableLogProvider();

        InterestingMonitor2 aMonitor = monitors.newMonitor( InterestingMonitor2.class );
        LoggingListener listener = new LoggingListener(
                Collections.<Class<?>, Logger>singletonMap( InterestingMonitor2.class, logProvider.getLog( InterestingMonitor2.class ).debugLogger() )
        );
        monitors.addMonitorListener( listener, listener.predicate );

        // When
        aMonitor.doubleTouch( "APA", 42 );

        // Then
        logProvider.assertExactly(
                inLog( InterestingMonitor2.class ).debug( "doubleTouch(String:APA,int:42)" )
        );
    }
}
