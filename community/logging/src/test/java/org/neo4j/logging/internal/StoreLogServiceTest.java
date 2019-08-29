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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.test.Race;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Long.max;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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
        try ( Lifespan life = new Lifespan( logService ) )
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
        try ( Lifespan life = new Lifespan( logService ) )
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
        try ( Lifespan life = new Lifespan( logService ) )
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
        shouldChangeLogLevelSettingStressfully( StoreLogService::setDefaultLogLevel );
    }

    @Test
    void shouldChangeSpecificLogLevelSettingStressfully() throws Throwable
    {
        shouldChangeLogLevelSettingStressfully( ( logService, level ) -> logService.setContextLogLevels(
                Stream.of( level ).collect( HashMap::new, ( map, lvl ) -> map.put( StoreLogServiceTest.class.getName(), lvl ), HashMap::putAll ) ) );
    }

    private void shouldChangeLogLevelSettingStressfully( BiConsumer<StoreLogService,Level> levelChanger ) throws Throwable
    {
        // given
        File file = directory.file( "log" );
        StoreLogService logService = StoreLogService
                .withInternalLog( file )
                .withDefaultLevel( Level.DEBUG )
                .build( fs );

        // when
        AtomicLong nextId = new AtomicLong();
        Map<Level,Long> idAtLevelChange = new ConcurrentHashMap<>();
        int loggerThreads = 4;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // Sometimes instantiate the log here.
        // Also sometimes not since we want to exercise the race of instantiating the log instance concurrently with changing log level.
        Log globalLog = random.nextBoolean() ? logService.getInternalLog( StoreLogServiceTest.class ) : null;
        try ( Lifespan life = new Lifespan( logService ) )
        {
            Race race = new Race();
            AtomicBoolean end = new AtomicBoolean();
            race.addContestants( loggerThreads, () ->
            {
                ThreadLocalRandom tlRandom = ThreadLocalRandom.current();
                while ( !end.get() )
                {
                    long id = nextId.incrementAndGet();
                    String message = String.valueOf( id );
                    // If global log is available then sometimes use it, otherwise pick from log service directly
                    Log log = globalLog != null && tlRandom.nextBoolean() ? globalLog : logService.getInternalLog( StoreLogServiceTest.class );
                    log.debug( message );
                    log.info( message );
                    log.warn( message );
                    log.error( message );
                }
            } );
            race.addContestant( throwing( () ->
            {
                ThreadLocalRandom tlRandom = ThreadLocalRandom.current();
                Level[] levels = new Level[]{Level.INFO, Level.WARN, Level.ERROR};
                for ( Level level : levels )
                {
                    Thread.sleep( tlRandom.nextInt( 50 ) );
                    levelChanger.accept( logService, level );
                    long idAtChange = nextId.get();
                    idAtLevelChange.put( level, idAtChange );
                }
                Thread.sleep( tlRandom.nextInt( 50 ) );
                end.set( true );
            } ) );
            race.go();
        }

        // then
        long[] highestIdForLevel = new long[Level.values().length];
        for ( String line : Files.readAllLines( file.toPath() ) )
        {
            Level level = logLevelOfLine( line );
            highestIdForLevel[level.ordinal()] = max( highestIdForLevel[level.ordinal()], idOfLine( line ) );
        }
        for ( Level level : new Level[]{Level.DEBUG, Level.INFO, Level.WARN} )
        {
            // highest of a level is roughly id at change of next higher level
            long idAtChange = idAtLevelChange.get( Level.values()[level.ordinal() + 1] );
            long highestForLevel = highestIdForLevel[level.ordinal()];
            long diff = Math.abs( highestForLevel - idAtChange );
            if ( diff > loggerThreads )
            {
                fail( format( "Diff too large for level:%s, changed at id:%d, but highest seen:%d", level, idAtChange, highestForLevel ) );
            }
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
