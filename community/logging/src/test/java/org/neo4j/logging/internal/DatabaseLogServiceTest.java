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
package org.neo4j.logging.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.logging.Level.DEBUG;
import static org.neo4j.logging.Level.ERROR;
import static org.neo4j.logging.Level.WARN;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class DatabaseLogServiceTest
{
    private static final String TEST_PREFIX = "prefix";

    @Inject
    private SuppressOutput suppressOutput;

    private FormattedLogProvider formattedLogProvider;
    private DatabaseLogService logService;

    @BeforeEach
    void setUp()
    {
        formattedLogProvider = FormattedLogProvider.withDefaultLogLevel( DEBUG ).toOutputStream( System.out );
        logService = new DatabaseLogService( DatabaseLogServiceTest::testLogContext, new SimpleLogService( formattedLogProvider ) );
    }

    @Test
    void shouldReturnUserLogProvider()
    {
        var logProvider = logService.getUserLogProvider();
        var log = logProvider.getLog( "log_name" );
        log.info( "message" );

        assertLogged( "[log_name] [prefix] message" );
    }

    @Test
    void shouldReturnInternalLogProvider()
    {
        var logProvider = logService.getInternalLogProvider();
        var log = logProvider.getLog( Object.class );
        log.info( "message" );

        assertLogged( "[j.l.Object] [prefix] message" );
    }

    @Test
    void shouldReturnDifferentUserAndInternalLogProviders()
    {
        var userLogProvider = logService.getUserLogProvider();
        var internalLogProvider = logService.getInternalLogProvider();

        assertNotEquals( userLogProvider, internalLogProvider );
    }

    @Test
    void shouldAlwaysReturnSameUserLogProvider()
    {
        var logProvider1 = logService.getUserLogProvider();
        var logProvider2 = logService.getUserLogProvider();

        assertSame( logProvider1, logProvider2 );
    }

    @Test
    void shouldAlwaysReturnSameInternalLogProvider()
    {
        var logProvider1 = logService.getInternalLogProvider();
        var logProvider2 = logService.getInternalLogProvider();

        assertSame( logProvider1, logProvider2 );
    }

    @Test
    void shouldSupportDynamicLogLevelChangesOfTheDelegate()
    {
        var log = logService.getUserLogProvider().getLog( "log_name" );

        // enable all levels
        formattedLogProvider.setDefaultLevel( DEBUG );
        log.debug( "message 1" );

        // disable all levels except error
        formattedLogProvider.setDefaultLevel( ERROR );
        log.info( "message 2" );

        // enable only warn and error
        formattedLogProvider.setDefaultLevel( WARN );
        log.warn( "message 3" );

        // enable all levels except debug
        formattedLogProvider.setDefaultLevel( WARN );
        log.debug( "message 4" );

        // enable all levels
        formattedLogProvider.setDefaultLevel( DEBUG );
        log.debug( "message 5" );

        assertLogged( "message 1" );
        assertNotLogged( "message 2" );
        assertLogged( "message 3" );
        assertNotLogged( "message 4" );
        assertLogged( "message 5" );
    }

    @Test
    void shouldSupportBulkLog()
    {
        var log = logService.getUserLogProvider().getLog( String.class );

        log.bulk( innerLog ->
        {
            innerLog.info( "info message" );
            innerLog.debug( "debug message" );
            innerLog.error( "error message" );
        } );

        assertLogged( "INFO [j.l.String] [prefix] info message" );
        assertLogged( "DEBUG [j.l.String] [prefix] debug message" );
        assertLogged( "ERROR [j.l.String] [prefix] error message" );
    }

    @Test
    void shouldSupportBulkLogger()
    {
        var logger = logService.getUserLogProvider().getLog( "TheLogger" ).warnLogger();

        logger.bulk( innerLogger ->
        {
            innerLogger.log( "message 1" );
            innerLogger.log( "message 2" );
            innerLogger.log( "message 3" );
        } );

        assertLogged( "WARN [TheLogger] [prefix] message 1" );
        assertLogged( "WARN [TheLogger] [prefix] message 2" );
        assertLogged( "WARN [TheLogger] [prefix] message 3" );
    }

    @Test
    void shouldNotLogPrefixWhenContextIsNull()
    {
        logService = new DatabaseLogService( null, new SimpleLogService( formattedLogProvider ) );
        var log = logService.getUserLogProvider().getLog( "MyLog" );

        log.info( "info message" );

        assertLogged( "INFO [MyLog] info message" );
    }

    private void assertLogged( String message )
    {
        assertTrue( suppressOutput.getOutputVoice().containsMessage( message ) );
    }

    private void assertNotLogged( String message )
    {
        assertFalse( suppressOutput.getOutputVoice().containsMessage( message ) );
    }

    private static String testLogContext( String message )
    {
        return "[" + TEST_PREFIX + "] " + message;
    }
}
