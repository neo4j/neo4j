/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.neo4j.helpers.FakeClock;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.TargetDirectory;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.Predicates.and;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;
import static org.neo4j.kernel.impl.util.StringLogger.DEFAULT_NAME;
import static org.neo4j.kernel.impl.util.StringLogger.DEFAULT_THRESHOLD_FOR_ROTATION;

public class StringLoggerTest
{
    private final FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();

    @Test
    public void makeSureLogsAreRotated() throws Exception
    {
        String path = "target/test-data/stringlogger";
        deleteRecursively( new File( path ) );
        File logFile = new File( path, StringLogger.DEFAULT_NAME );
        File oldFile = new File( path, StringLogger.DEFAULT_NAME + ".1" );
        File oldestFile = new File( path, StringLogger.DEFAULT_NAME + ".2" );
        StringLogger logger = StringLogger.loggerDirectory( fileSystem,
                new File( path ), 200 * 1024, false );
        assertFalse( fileSystem.fileExists( oldFile ) );
        int counter = 0;
        String prefix = "Bogus message ";

        // First rotation
        while ( !fileSystem.fileExists( oldFile ) )
        {
            logger.logMessage( prefix + counter++, true );
        }
        int mark1 = counter-1;
        logger.logMessage( prefix + counter++, true );
        assertTrue( firstLineOfFile( oldFile ).contains( prefix + "0" ) );
        assertTrue( lastLineOfFile( oldFile ).first().contains( prefix + mark1 ) );
        assertTrue( firstLineOfFile( logFile ).contains( prefix + (counter-1) ) );

        // Second rotation
        while ( !fileSystem.fileExists( oldestFile ) )
        {
            logger.logMessage( prefix + counter++, true );
        }
        int mark2 = counter-1;
        logger.logMessage( prefix + counter++, true );
        assertTrue( firstLineOfFile( oldestFile ).contains( prefix + "0" ) );
        assertTrue( lastLineOfFile( oldestFile ).first().contains( prefix + mark1 ) );
        assertTrue( firstLineOfFile( oldFile ).contains( prefix + (mark1+1) ) );
        assertTrue( lastLineOfFile( oldFile ).first().contains( prefix + mark2 ) );
        assertTrue( firstLineOfFile( logFile ).contains( prefix + (counter-1) ) );

        // Third rotation, assert .2 file is now what used to be .1 used to be and
        // .3 doesn't exist
        long previousSize = 0;
        while ( true )
        {
            logger.logMessage( prefix + counter++, true );
            if ( fileSystem.getFileSize( logFile ) < previousSize )
            {
                break;
            }
            previousSize = fileSystem.getFileSize( logFile );
        }
        assertFalse( fileSystem.fileExists( new File( path, StringLogger.DEFAULT_NAME + ".3" ) ) );
        assertTrue( firstLineOfFile( oldestFile ).contains( prefix + (mark1+1) ) );
        assertTrue( lastLineOfFile( oldestFile ).first().contains( prefix + mark2 ) );
    }

    @Test
    public void makeSureRotationDoesNotRecurse() throws Exception
    {
        final String baseMessage = "base message";
        File target = TargetDirectory.forTest( StringLoggerTest.class ).cleanDirectory( "recursionTest" );
        final StringLogger logger = StringLogger.loggerDirectory( fileSystem, target,
                baseMessage.length() /*rotation threshold*/, false );

        /*
         * The trigger that will log more than the threshold during rotation, possibly causing another rotation
         */
        Runnable trigger = new Runnable()
        {
            @Override
            public void run()
            {
                logger.logMessage( baseMessage + " from trigger", true );
            }
        };
        logger.addRotationListener( trigger );
        logger.logMessage( baseMessage + " from main", true );

        File rotated = new File( target, "messages.log.1" );
        assertTrue( "rotated file not present, should have been created", fileSystem.fileExists( rotated ) );

        Pair<String, Integer> rotatedInfo = lastLineOfFile( rotated );
        assertTrue( "rotated file should have only stuff from main", rotatedInfo.first().endsWith( " from main" )
                                                                     && rotatedInfo.other() == 1 );

        File current = new File( target, "messages.log" );
        assertTrue( "should have created a new messages.log file", fileSystem.fileExists( current ) );
        Pair<String, Integer> currentInfo = lastLineOfFile( current );
        assertTrue( "current file should have only stuff from trigger", currentInfo.first().endsWith( " from trigger" )
                                                                        && currentInfo.other() == 1 );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldLogDebugMessagesIfToldTo() throws Exception
    {
        // GIVEN
        File target = TargetDirectory.forTest( StringLoggerTest.class ).cleanDirectory( "debug" );
        StringLogger logger = StringLogger.loggerDirectory( fileSystem, target, DEFAULT_THRESHOLD_FOR_ROTATION, true );

        // WHEN
        String firstMessage = "First message";
        String secondMessage = "Second message";
        String thirdMessage = "Third message";
        logger.debug( firstMessage );
        logger.debug( secondMessage, new RuntimeException( thirdMessage ) );
        logger.close();

        // THEN
        File logFile = new File( target, DEFAULT_NAME );
        assertTrue( "Should have contained " + firstMessage, fileContains( logFile, stringContaining( firstMessage ) ) );
        assertTrue( "Should have contained " + secondMessage, fileContains( logFile, stringContaining( secondMessage ) ) );
        assertTrue( "Should have contained " + thirdMessage, fileContains( logFile, stringContaining( thirdMessage ) ) );
        assertTrue( "Should have contained stack trace from " + thirdMessage, fileContains( logFile, and(
                stringContaining( "at " ), stringContaining( testName.getMethodName() ) ) ) );
    }

    @Test
    public void cappedLoggerShouldIgnoreSubsequentMessagesWithinTimeInterval()
    {
        StringBuffer buffer = new StringBuffer();
        StringLogger delegate = StringLogger.wrap( buffer );
        FakeClock fakeClock = new FakeClock();
        StringLogger cappedLogger = StringLogger.cappedLogger( delegate,
                CappedOperation.<String>time( fakeClock, 1, TimeUnit.MILLISECONDS ) );

        fakeClock.forward( 1, TimeUnit.MILLISECONDS );
        cappedLogger.info( "f1rst" );
        cappedLogger.info( "s3cond" );
        fakeClock.forward( 1, TimeUnit.MILLISECONDS );
        cappedLogger.info( "th1rd" );

        String output = buffer.toString();
        assertThat( output, containsString( "f1rst" ) );
        assertThat( output, containsString( "th1rd" ) );
        assertThat( output, not( containsString( "s3cond" ) ) );
    }

    private Predicate<String> stringContaining( final String string )
    {
        return new Predicate<String>()
        {
            @Override
            public boolean accept( String item )
            {
                return item.contains( string );
            }
        };
    }

    private String firstLineOfFile( File file ) throws Exception
    {
        BufferedReader reader = new BufferedReader( fileSystem.openAsReader( file, Charset.defaultCharset().name() ) );
        String result = reader.readLine();
        reader.close();
        return result;
    }

    private boolean fileContains( File file, Predicate<String> predicate ) throws IOException
    {
        BufferedReader reader = new BufferedReader( fileSystem.openAsReader( file, Charset.defaultCharset().name() ) );
        try
        {
            String line = null;
            while ( (line = reader.readLine()) != null )
            {
                if ( predicate.accept( line ) )
                {
                    return true;
                }
            }
            return false;
        }
        finally
        {
            reader.close();
        }
    }

    /*
     * Returns a Pair of the last line in the file and the number of lines in the file, so the
     * other part from a one line file will be 1  and the other part from an empty file 0.
     */
    private Pair<String, Integer> lastLineOfFile( File file ) throws Exception
    {
        int count = 0;
        BufferedReader reader = new BufferedReader( fileSystem.openAsReader( file, Charset.defaultCharset().name() ) );
        String line = null;
        String result = null;
        while ( (line = reader.readLine()) != null )
        {
            result = line;
            count++;
        }
        reader.close();
        return Pair.of( result, count );
    }

    public final @Rule TestName testName = new TestName();
}
