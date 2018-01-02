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
package org.neo4j.kernel.monitoring;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.junit.Test;

public class MonitorsTest
{
    interface MyMonitor
    {
        void aVoid();
        void takesArgs( String arg1, long arg2, Object ... moreArgs );
    }

    @Test
    public void shouldProvideNoOpDelegate() throws Exception
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
    public void shouldRegister() throws Exception
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
    public void shouldUnregister() throws Exception
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
    public void shouldRespectTags() throws Exception
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
}
