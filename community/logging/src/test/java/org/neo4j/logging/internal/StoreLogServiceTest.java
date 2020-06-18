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
package org.neo4j.logging.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.test.Race;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Long.max;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.Race.throwing;

@ExtendWith( DefaultFileSystemExtension.class )
@TestDirectoryExtension
class StoreLogServiceTest
{
    @Inject
    DefaultFileSystemAbstraction fs;

    @Inject
    TestDirectory directory;

    @Test
    void shouldReactToChangeInLogLevel() throws IOException
    {
        // given
        File file = directory.file( "log" );
        StoreLogService logService = StoreLogService
                .withInternalLog( file )
                .withDefaultLevel( Level.INFO )
                .build( fs );
        String firstMessage = "FirstMessage";
        String secondMessage = "SecondMessage";
        try ( Lifespan ignored = new Lifespan( logService ) )
        {
            Log log = logService.getInternalLog( getClass() );
            log.debug( firstMessage );

            // when
            logService.setDefaultLogLevel( Level.DEBUG );
            log.debug( secondMessage );
        }

        // then
        assertFalse( logStatementFound( file, firstMessage ) );
        assertTrue( logStatementFound( file, secondMessage ) );
    }

    @Test
    void shouldReactToChangeInUserLogLevelIfOurLogProvider() throws IOException
    {
        // given
        File userLog = directory.file( "userlog" );
        FormattedLogProvider userLogProvider = FormattedLogProvider.toOutputStream( fs.openAsOutputStream( userLog, true ) );
        File file = directory.file( "log" );
        StoreLogService logService = StoreLogService
                .withInternalLog( file )
                .withUserLogProvider( userLogProvider )
                .withDefaultLevel( Level.INFO )
                .build( fs );
        String firstMessage = "FirstMessage";
        String secondMessage = "SecondMessage";
        String thirdMessage = "ThirdMessage";
        try ( Lifespan ignored = new Lifespan( logService ) )
        {
            Log log = logService.getUserLog( getClass() );
            log.debug( firstMessage );

            // when
            logService.setDefaultLogLevel( Level.DEBUG );
            log.debug( secondMessage );

            logService.setDefaultLogLevel( Level.INFO );
            log.debug( thirdMessage );
        }

        // then
        assertFalse( logStatementFound( userLog, firstMessage ) );
        assertTrue( logStatementFound( userLog, secondMessage ) );
        assertFalse( logStatementFound( userLog, thirdMessage ) );
    }

    @Test
    void shouldReactToChangeInContextLogLevels() throws IOException
    {
        // given
        File file = directory.file( "log" );
        StoreLogService logService = StoreLogService
                .withInternalLog( file )
                .withDefaultLevel( Level.INFO )
                .withLevel( "org.neo4j", Level.WARN )
                .build( fs );
        String firstMessage = "FirstMessage";
        String secondMessage = "SecondMessage";
        try ( Lifespan ignored = new Lifespan( logService ) )
        {
            Log log = logService.getInternalLog( getClass() );
            log.info( firstMessage );

            // when
            logService.setContextLogLevels( Map.of( "org.neo4j", Level.INFO ) );
            log.info( secondMessage );
        }

        // then
        assertFalse( logStatementFound( file, firstMessage ) );
        assertTrue( logStatementFound( file, secondMessage ) );
    }

    @Test
    void shouldPickLowestLevelHit() throws IOException
    {
        // given
        File file = directory.file( "log" );
        StoreLogService logService = StoreLogService
                .withInternalLog( file )
                .withDefaultLevel( Level.INFO )
                .withLevel( "org.neo4j", Level.WARN )
                .withLevel( getClass().getPackage().getName(), Level.DEBUG )
                .build( fs );
        String firstMessage = "FirstMessage";
        try ( Lifespan ignored = new Lifespan( logService ) )
        {
            Log log = logService.getInternalLog( getClass() );
            log.debug( firstMessage );
        }

        // then
        assertTrue( logStatementFound( file, firstMessage ) );
    }

    @Test
    void shouldChangeDefaultLogLevelSettingStressfully() throws Throwable
    {
        shouldChangeLogLevelSettingStressfully( StoreLogService::setDefaultLogLevel, false );
    }

    @Test
    void shouldChangeDefaultUserLogLevelSettingStressfully() throws Throwable
    {
        shouldChangeLogLevelSettingStressfully( StoreLogService::setDefaultLogLevel, true );
    }

    @Test
    void shouldChangeSpecificLogLevelSettingStressfully() throws Throwable
    {
        shouldChangeLogLevelSettingStressfully( ( logService, level ) -> logService.setContextLogLevels(
                Stream.of( level ).collect( HashMap::new, ( map, lvl ) -> map.put( StoreLogServiceTest.class.getName(), lvl ), HashMap::putAll ) ), false );
    }

    private Log getLogFromLogService( StoreLogService logService, boolean getUserLogProvider )
    {
        if ( !getUserLogProvider )
        {
            return logService.getInternalLog( StoreLogServiceTest.class );
        }
        return logService.getUserLog( StoreLogServiceTest.class );
    }

    private void shouldChangeLogLevelSettingStressfully( BiConsumer<StoreLogService,Level> levelChanger, boolean testUserLogProvider ) throws Throwable
    {
        // given
        File file = directory.file( "log" );
        File userFile = directory.file( "userlog" );
        StoreLogService logService = StoreLogService
                .withInternalLog( file )
                .withUserLogProvider( FormattedLogProvider.toOutputStream( fs.openAsOutputStream( userFile, true ) ) )
                .withDefaultLevel( Level.DEBUG )
                .build( fs );

        // when
        AtomicLong nextId = new AtomicLong();
        int loggerThreads = 4;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // Sometimes instantiate the log here.
        // Also sometimes not since we want to exercise the race of instantiating the log instance concurrently with changing log level.
        Log globalLog = random.nextBoolean() ? getLogFromLogService( logService, testUserLogProvider ) : null;
        AtomicInteger logRequestsMade = new AtomicInteger();
        try ( Lifespan ignored = new Lifespan( logService ) )
        {
            Race race = new Race();
            AtomicBoolean end = new AtomicBoolean();
            race.addContestants( loggerThreads, () ->
            {
                ThreadLocalRandom tlRandom = ThreadLocalRandom.current();
                int messagesAfterEnd = 0;
                while ( !end.get() || messagesAfterEnd++ < 10 )
                {
                    long id = nextId.incrementAndGet();
                    String message = String.valueOf( id );
                    // If global log is available then sometimes use it, otherwise pick from log service directly
                    Log log = globalLog != null && tlRandom.nextBoolean() ? globalLog : getLogFromLogService( logService, testUserLogProvider );
                    log.debug( message );
                    log.info( message );
                    log.warn( message );
                    log.error( message );
                    logRequestsMade.incrementAndGet();
                }
            } );
            race.addContestant( throwing( () ->
            {
                Level[] levels = new Level[]{Level.INFO, Level.WARN, Level.ERROR};
                for ( Level level : levels )
                {
                    int count = logRequestsMade.get();
                    while ( count == logRequestsMade.get() )
                    {
                        Thread.sleep( 10 );
                    }
                    levelChanger.accept( logService, level );
                }
                end.set( true );
            } ) );
            race.go();
        }

        // then
        long[] highestIdForLevel = new long[Level.values().length];
        for ( String line : Files.readAllLines( testUserLogProvider ? userFile.toPath() : file.toPath() ) )
        {
            Level level = logLevelOfLine( line );
            highestIdForLevel[level.ordinal()] = max( highestIdForLevel[level.ordinal()], idOfLine( line ) );
        }
        for ( Level level : new Level[]{Level.DEBUG, Level.INFO, Level.WARN} )
        {
            long highestForLevel = highestIdForLevel[level.ordinal()];
            assertThat( highestForLevel, greaterThan( 0L ) );
        }
        assertEquals( nextId.get(), highestIdForLevel[Level.ERROR.ordinal()] );
    }

    private long idOfLine( String line )
    {
        int lastSpaceIndex = line.lastIndexOf( ' ' );
        assertTrue( lastSpaceIndex != -1 );
        String idString = line.substring( lastSpaceIndex + 1 );
        return Long.parseLong( idString );
    }

    private Level logLevelOfLine( String line )
    {
        // find the + (time zone thingie)
        int plusIndex = line.indexOf( '+' );
        assertTrue( plusIndex != -1 );
        // find the [ (time package thingie)
        int bracketIndex = line.indexOf( '[', plusIndex );
        assertTrue( bracketIndex != -1 );
        // now we know where the log level is
        String levelString = line.substring( plusIndex + 6, bracketIndex - 1 );
        return Level.valueOf( levelString );
    }

    private boolean logStatementFound( File file, String message ) throws IOException
    {
        return Files.readAllLines( file.toPath() ).stream().anyMatch( line -> line.contains( message ) );
    }
}
