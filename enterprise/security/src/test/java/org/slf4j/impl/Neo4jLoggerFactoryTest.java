/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.slf4j.impl;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class Neo4jLoggerFactoryTest
{
    private final String format = "hello %s, %s, %s";
    private final String arg1 = "1";
    private final String arg2 = "2";
    private final String arg3 = "3";
    private final String[] args = { arg1, arg2, arg3 };

    private final LogProvider neo4jLogProvider = mock( LogProvider.class );
    private final Log neo4jLog = mock( Log.class );
    private final Marker marker = mock( Marker.class );

    @Before
    public void setUp()
    {
        when( neo4jLogProvider.getLog( anyString() ) ).thenReturn( neo4jLog );
    }

    @Test
    public void shouldSupporIsDebugEnabled()
    {
        // Given
        StaticLoggerBinder.setNeo4jLogProvider( neo4jLogProvider );
        Logger slf4jLog = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );
        when( neo4jLog.isDebugEnabled() ).thenReturn( true );

        // When
        boolean debugEnabled = slf4jLog.isDebugEnabled();
        when( neo4jLog.isDebugEnabled() ).thenReturn( false );
        boolean debugDisabled = slf4jLog.isDebugEnabled();

        // Then
        verify( neo4jLog, times(2) ).isDebugEnabled();
        assertTrue( debugEnabled );
        assertFalse( debugDisabled );
    }

    @Test
    public void shouldSupporDebug()
    {
        // Given
        StaticLoggerBinder.setNeo4jLogProvider( neo4jLogProvider );
        Logger slf4jLog = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );

        // When
        slf4jLog.debug( format );
        slf4jLog.debug( format, arg1 );
        slf4jLog.debug( format, arg1, arg2 );
        slf4jLog.debug( format, args );

        // Then
        verify( neo4jLog ).debug( format );
        verify( neo4jLog ).debug( format, arg1 );
        verify( neo4jLog ).debug( format, arg1, arg2 );
        verify( neo4jLog ).debug( format, arg1, arg2, arg3 );
    }

    @Test
    public void shouldSupporDebugWithMarkerIgnored()
    {
        // Given
        StaticLoggerBinder.setNeo4jLogProvider( neo4jLogProvider );
        Logger slf4jLog = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );

        // When
        slf4jLog.debug( marker, format );
        slf4jLog.debug( marker, format, arg1 );
        slf4jLog.debug( marker, format, arg1, arg2 );
        slf4jLog.debug( marker, format, args );

        // Then
        verify( neo4jLog ).debug( format );
        verify( neo4jLog ).debug( format, arg1 );
        verify( neo4jLog ).debug( format, arg1, arg2 );
        verify( neo4jLog ).debug( format, arg1, arg2, arg3 );
    }

    @Test
    public void shouldSupporInfo()
    {
        // Given
        StaticLoggerBinder.setNeo4jLogProvider( neo4jLogProvider );
        Logger slf4jLog = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );

        // When
        slf4jLog.info( format );
        slf4jLog.info( format, arg1 );
        slf4jLog.info( format, arg1, arg2 );
        slf4jLog.info( format, args );

        // Then
        verify( neo4jLog ).info( format );
        verify( neo4jLog ).info( format, arg1 );
        verify( neo4jLog ).info( format, arg1, arg2 );
        verify( neo4jLog ).info( format, arg1, arg2, arg3 );
    }

    @Test
    public void shouldSupporInfoWithMarkerIgnored()
    {
        // Given
        StaticLoggerBinder.setNeo4jLogProvider( neo4jLogProvider );
        Logger slf4jLog = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );

        // When
        slf4jLog.info( marker, format );
        slf4jLog.info( marker, format, arg1 );
        slf4jLog.info( marker, format, arg1, arg2 );
        slf4jLog.info( marker, format, args );

        // Then
        verify( neo4jLog ).info( format );
        verify( neo4jLog ).info( format, arg1 );
        verify( neo4jLog ).info( format, arg1, arg2 );
        verify( neo4jLog ).info( format, arg1, arg2, arg3 );
    }

    @Test
    public void shouldSupporWarn()
    {
        // Given
        StaticLoggerBinder.setNeo4jLogProvider( neo4jLogProvider );
        Logger slf4jLog = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );

        // When
        slf4jLog.warn( format );
        slf4jLog.warn( format, arg1 );
        slf4jLog.warn( format, arg1, arg2 );
        slf4jLog.warn( format, args );

        // Then
        verify( neo4jLog ).warn( format );
        verify( neo4jLog ).warn( format, arg1 );
        verify( neo4jLog ).warn( format, arg1, arg2 );
        verify( neo4jLog ).warn( format, arg1, arg2, arg3 );
    }

    @Test
    public void shouldSupporWarnWithMarkerIgnored()
    {
        // Given
        StaticLoggerBinder.setNeo4jLogProvider( neo4jLogProvider );
        Logger slf4jLog = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );

        // When
        slf4jLog.warn( marker, format );
        slf4jLog.warn( marker, format, arg1 );
        slf4jLog.warn( marker, format, arg1, arg2 );
        slf4jLog.warn( marker, format, args );

        // Then
        verify( neo4jLog ).warn( format );
        verify( neo4jLog ).warn( format, arg1 );
        verify( neo4jLog ).warn( format, arg1, arg2 );
        verify( neo4jLog ).warn( format, arg1, arg2, arg3 );
    }

    @Test
    public void shouldSupporError()
    {
        // Given
        StaticLoggerBinder.setNeo4jLogProvider( neo4jLogProvider );
        Logger slf4jLog = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );

        // When
        slf4jLog.error( format );
        slf4jLog.error( format, arg1 );
        slf4jLog.error( format, arg1, arg2 );
        slf4jLog.error( format, args );

        // Then
        verify( neo4jLog ).error( format );
        verify( neo4jLog ).error( format, arg1 );
        verify( neo4jLog ).error( format, arg1, arg2 );
        verify( neo4jLog ).error( format, arg1, arg2, arg3 );
    }

    @Test
    public void shouldSupporErrorWithMarkerIgnored()
    {
        // Given
        StaticLoggerBinder.setNeo4jLogProvider( neo4jLogProvider );
        Logger slf4jLog = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );

        // When
        slf4jLog.error( marker, format );
        slf4jLog.error( marker, format, arg1 );
        slf4jLog.error( marker, format, arg1, arg2 );
        slf4jLog.error( marker, format, args );

        // Then
        verify( neo4jLog ).error( format );
        verify( neo4jLog ).error( format, arg1 );
        verify( neo4jLog ).error( format, arg1, arg2 );
        verify( neo4jLog ).error( format, arg1, arg2, arg3 );
    }

    @Test
    public void shouldSupporChangingLogProvider()
    {
        // Given
        LogProvider neo4jLogProvider2 = mock( LogProvider.class );
        Log neo4jLog2 = mock( Log.class );
        when( neo4jLogProvider2.getLog( anyString() ) ).thenReturn( neo4jLog2 );

        StaticLoggerBinder.setNeo4jLogProvider( neo4jLogProvider );
        Logger slf4jLog = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );

        // When
        slf4jLog.error( format );
        StaticLoggerBinder.setNeo4jLogProvider( neo4jLogProvider2 );
        Logger slf4jLog2 = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );
        slf4jLog2.error( format );
        StaticLoggerBinder.setNeo4jLogProvider( null );
        Logger slf4jLog3 = LoggerFactory.getLogger( Neo4jLoggerFactoryTest.class );
        slf4jLog3.error( format );

        // Then
        verify( neo4jLog ).error( format );
        verify( neo4jLog2 ).error( format );
    }
}
