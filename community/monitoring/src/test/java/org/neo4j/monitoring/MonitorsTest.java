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
package org.neo4j.monitoring;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

class MonitorsTest
{
    interface MyMonitor
    {
        void aVoid();
        void takesArgs( String arg1, long arg2, Object ... moreArgs );
    }

    @Test
    void shouldProvideNoOpDelegate()
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
    void shouldRegister()
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
        Mockito.verify(listener).aVoid();
        Mockito.verify(listener).takesArgs( "ha", 12, obj );
    }

    @Test
    void shouldUnregister()
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
        Mockito.verifyNoMoreInteractions( listener );
    }

    @Test
    void shouldRespectTags()
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
        Mockito.verifyZeroInteractions( listener );
        monitorTag2.aVoid();
        Mockito.verify( listener ).aVoid();
        Mockito.verifyNoMoreInteractions( listener );
    }

    @Test
    void eventShouldBubbleUp()
    {
        Monitors parent = new Monitors();
        MyMonitor parentListener = mock( MyMonitor.class );
        parent.addMonitorListener( parentListener );

        Monitors child = new Monitors( parent );
        MyMonitor childListener = mock( MyMonitor.class );
        child.addMonitorListener( childListener );

        // Calls on monitors from parent should not reach child listeners
        MyMonitor parentMonitor = parent.newMonitor( MyMonitor.class );
        parentMonitor.aVoid();
        Mockito.verify( parentListener ).aVoid();
        Mockito.verifyZeroInteractions( childListener );

        // Calls on monitors from child should reach both listeners
        MyMonitor childMonitor = child.newMonitor( MyMonitor.class );
        childMonitor.aVoid();
        Mockito.verify( parentListener, Mockito.times( 2 ) ).aVoid();
        Mockito.verify( childListener ).aVoid();
    }
}
