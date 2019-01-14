/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.monitoring;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class MonitorsTest
{
    interface MyMonitor
    {
        void aVoid();
        void takesArgs( String arg1, long arg2, Object ... moreArgs );
    }

    @Test
    public void shouldProvideNoOpDelegate()
    {
        // Given
        Monitors monitors = new Monitors();

        // When
        MyMonitor monitor = monitors.newMonitor( MyMonitor.class );

        // Then those should be no-ops
        monitor.aVoid();
        monitor.takesArgs( "ha", 12, new Object() );
    }

    @Test
    public void shouldRegister()
    {
        // Given
        Monitors monitors = new Monitors();

        MyMonitor listener = mock( MyMonitor.class );
        MyMonitor monitor = monitors.newMonitor( MyMonitor.class );
        Object obj = new Object();

        // When
        monitors.addMonitorListener( listener );
        monitor.aVoid();
        monitor.takesArgs( "ha", 12, obj );

        // Then
        verify(listener).aVoid();
        verify(listener).takesArgs( "ha", 12, obj );
    }

    @Test
    public void shouldUnregister()
    {
        // Given
        Monitors monitors = new Monitors();

        MyMonitor listener = mock( MyMonitor.class );
        MyMonitor monitor = monitors.newMonitor( MyMonitor.class );
        Object obj = new Object();

        monitors.addMonitorListener( listener );

        // When
        monitors.removeMonitorListener( listener );
        monitor.aVoid();
        monitor.takesArgs( "ha", 12, obj );

        // Then
        verifyNoMoreInteractions( listener );
    }

    @Test
    public void shouldRespectTags()
    {
        // Given
        Monitors monitors = new Monitors();

        MyMonitor listener = mock( MyMonitor.class );
        MyMonitor monitorTag1 = monitors.newMonitor( MyMonitor.class, "tag1" );
        MyMonitor monitorTag2 = monitors.newMonitor( MyMonitor.class, "tag2" );

        // When
        monitors.addMonitorListener( listener, "tag2" );

        // Then
        monitorTag1.aVoid();
        verifyZeroInteractions( listener );
        monitorTag2.aVoid();
        verify( listener, times(1) ).aVoid();
        verifyNoMoreInteractions( listener );
    }

    @Test
    public void shouldTellIfMonitorHasListeners()
    {
        // Given
        Monitors monitors = new Monitors();
        MyMonitor listener = mock( MyMonitor.class );

        // When I have a monitor with no listeners
        monitors.newMonitor( MyMonitor.class );

        // Then
        assertFalse( monitors.hasListeners( MyMonitor.class ) );

        // When I add a listener
        monitors.addMonitorListener( listener );

        // Then
        assertTrue( monitors.hasListeners( MyMonitor.class ) );

        // When that listener is removed again
        monitors.removeMonitorListener( listener );

        // Then
        assertFalse( monitors.hasListeners( MyMonitor.class ) );
    }

    @Test
    public void exceptionsInHandlersAreLogged()
    {
        // given
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStream outputStream = new PrintStream( byteArrayOutputStream );
        LogProvider logProvider = FormattedLogProvider.toOutputStream( outputStream );
        Monitors monitors = new Monitors( logProvider );

        // and
        MyMonitor listener = mock( MyMonitor.class );
        RuntimeException runtimeException = new RuntimeException( "Exception message" );
        doThrow( runtimeException ).when( listener ).aVoid();
        monitors.addMonitorListener( listener );

        // when
        MyMonitor monitor = monitors.newMonitor( MyMonitor.class );
        monitor.aVoid();

        // then
        String logOutput = byteArrayOutputStream.toString();
        assertTrue( logOutput.contains( "RuntimeException: Exception message" ) );
        assertTrue( logOutput.contains( this.getClass().getName() ) );
        assertTrue( logOutput.contains( "Encountered exception while handling listener for monitor method aVoid" ) );
    }
}
