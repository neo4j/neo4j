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
package org.neo4j.kernel.logging;

import org.junit.Test;

import org.neo4j.kernel.impl.util.StringLogger;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static org.neo4j.kernel.logging.LogMarker.CONSOLE_MARK;

public class ConsoleLoggerTest
{
    @Test
    public void shouldAddConsoleMarkToInfo() throws Exception
    {
        // GIVEN
        StringLogger realLogger = mock( StringLogger.class );
        ConsoleLogger logger = new ConsoleLogger( realLogger );

        // WHEN
        logger.log( "A1" );
        logger.log( "A%d", 2 );

        // THEN
        verify( realLogger ).info( eq( "A1" ), any( Throwable.class ), anyBoolean(), eq( CONSOLE_MARK ) );
        verify( realLogger ).info( eq( "A2" ), any( Throwable.class ), anyBoolean(), eq( CONSOLE_MARK ) );
    }

    @Test
    public void shouldAddConsoleMarkToWarn() throws Exception
    {
        // GIVEN
        StringLogger realLogger = mock( StringLogger.class );
        ConsoleLogger logger = new ConsoleLogger( realLogger );
        Exception cause = new RuntimeException( "cause" );

        // WHEN
        logger.warn( "A1" );
        logger.warn( "A%d", 2 );
        logger.warn( "A3", cause );

        // THEN
        verify( realLogger ).warn( eq( "A1" ), any( Throwable.class ), anyBoolean(), eq( CONSOLE_MARK ) );
        verify( realLogger ).warn( eq( "A2" ), any( Throwable.class ), anyBoolean(), eq( CONSOLE_MARK ) );
        verify( realLogger ).warn( eq( "A3" ), eq( cause ), anyBoolean(), eq( CONSOLE_MARK ) );
    }

    @Test
    public void shouldAddConsoleMarkToError() throws Exception
    {
        // GIVEN
        StringLogger realLogger = mock( StringLogger.class );
        ConsoleLogger logger = new ConsoleLogger( realLogger );
        Exception cause = new RuntimeException( "cause" );

        // WHEN
        logger.error( "A1" );
        logger.error( "A%d", 2 );
        logger.error( "A3", cause );

        // THEN
        verify( realLogger ).error( eq( "A1" ), any( Throwable.class ), anyBoolean(), eq( CONSOLE_MARK ) );
        verify( realLogger ).error( eq( "A2" ), any( Throwable.class ), anyBoolean(), eq( CONSOLE_MARK ) );
        verify( realLogger ).error( eq( "A3" ), eq( cause ), anyBoolean(), eq( CONSOLE_MARK ) );
    }
}
