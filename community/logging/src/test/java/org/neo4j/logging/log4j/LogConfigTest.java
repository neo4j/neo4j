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
package org.neo4j.logging.log4j;

import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.logging.FormattedLogFormat;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class LogConfigTest
{
    static final String DATE_PATTERN = "\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d[+-]\\d\\d\\d\\d";
    private static final String DATE_PATTERN_NO_TIMEZONE = "\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d";
    @Inject
    SuppressOutput suppressOutput;
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private TestDirectory dir;

    private Neo4jLoggerContext ctx;

    @AfterEach
    void tearDown()
    {
        ctx.close();
    }

    @Test
    void shouldRespectLogLevel()
    {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        ctx = LogConfig.createBuilder( outContent, Level.DEBUG )
                .build();

        Logger logger = ctx.getLogger( "org.neo4j.classname" );
        logger.debug( "test" );
        logger.info( "test" );
        logger.warn( "test" );
        logger.error( "test" );

        String output = outContent.toString();
        assertThat( output ).contains( Level.DEBUG.toString() );
        assertThat( output ).contains( Level.INFO.toString() );
        assertThat( output ).contains( Level.WARN.toString() );
        assertThat( output ).contains( Level.ERROR.toString() );

        outContent.reset();

        LogConfig.updateLogLevel( Level.WARN, ctx );

        logger.debug( "test" );
        logger.info( "test" );
        logger.warn( "test" );
        logger.error( "test" );

        output = outContent.toString();
        assertThat( output ).doesNotContain( Level.DEBUG.toString() );
        assertThat( output ).doesNotContain( Level.INFO.toString() );
        assertThat( output ).contains( Level.WARN.toString() );
        assertThat( output ).contains( Level.ERROR.toString() );
    }

    @Test
    void withRotationShouldRotateOnThreshold()
    {
        File targetFile = new File( dir.homeDir(), "debug.log" );
        File targetFile1 = new File( dir.homeDir(), "debug.log.1" );
        File targetFile2 = new File( dir.homeDir(), "debug.log.2" );

        ctx = LogConfig.createBuilder( targetFile.toPath(), Level.INFO )
                .withRotation( 10, 2 )
                .build();

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );

        Logger logger = ctx.getLogger( "test" );

        logger.warn( "test" );
        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile1 ) ).isEqualTo( false );

        logger.warn( "test" );
        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile1 ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile2 ) ).isEqualTo( false );
    }

    @Test
    void withRotationShouldRespectMaxArchives() throws IOException
    {
        File targetFile = new File( dir.homeDir(), "debug.log" );
        File targetFile1 = new File( dir.homeDir(), "debug.log.1" );
        File targetFile2 = new File( dir.homeDir(), "debug.log.2" );
        File targetFile3 = new File( dir.homeDir(), "debug.log.3" );

        ctx = LogConfig.createBuilder( targetFile.toPath(), Level.INFO )
                .withRotation( 10, 2 )
                .build();

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );

        Logger logger = ctx.getLogger( "test" );

        logger.warn( "test1" );
        logger.warn( "test2" );
        logger.warn( "test3" );
        logger.warn( "test4" );
        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile1 ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile2 ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile3 ) ).isEqualTo( false );

        assertThat( Files.readString( targetFile.toPath() ) ).contains( "test4" );
        assertThat( Files.readString( targetFile1.toPath() ) ).contains( "test3" );
        assertThat( Files.readString( targetFile2.toPath() ) ).contains( "test2" );
    }

    @Test
    void withHeaderLoggerShouldBeUsedAsHeader() throws IOException
    {
        File targetFile = new File( dir.homeDir(), "debug.log" );
        File targetFile1 = new File( dir.homeDir(), "debug.log.1" );

        ctx = LogConfig.createBuilder( targetFile.toPath(), Level.INFO )
                .withRotation( 30, 2 )
                .withHeaderLogger( log ->
                        {
                            log.warn( "My Header" );
                            log.warn( "In Two Lines" );
                        }, "org.neo4j.HeaderClassName" )
                .build();

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );

        Logger logger = ctx.getLogger( "className" );

        logger.warn( "Long line that will get next message to be written to next file" );
        logger.warn( "test2" );

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile1 ) ).isEqualTo( true );

        // First file (the one rotated to targetFile1) should not have the header.
        assertThat( Files.readString( targetFile1.toPath() ) )
                .matches( DATE_PATTERN + format( " %-5s \\[className\\] Long line that will get next message to be written to next file%n", Level.WARN ) );

        assertThat( Files.readString( targetFile.toPath() ) )
                .matches( format( DATE_PATTERN + " %-5s \\[o.n.HeaderClassName\\] My Header%n" +
                                  DATE_PATTERN + " %-5s \\[o.n.HeaderClassName\\] In Two Lines%n" +
                                  DATE_PATTERN + " %-5s \\[className\\] test2%n", Level.WARN, Level.WARN, Level.WARN ) );
    }

    @Test
    void createOnDemandShouldCreateOnDemand()
    {
        File targetFile = new File( dir.homeDir(), "debug.log" );

        ctx = LogConfig.createBuilder( targetFile.toPath(), Level.INFO )
                .withRotation( 10, 2 )
                .createOnDemand()
                .build();

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( false );

        Logger logger = ctx.getLogger( "test" );
        logger.warn( "test" );

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );
    }

    @Test
    void withOutputStreamShouldLogToTheStream()
    {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        ctx = LogConfig.createBuilder( outContent, Level.INFO )
                .build();

        Logger logger = ctx.getLogger( "test" );
        logger.warn( "test" );

        assertThat( outContent.toString() ).contains( "test" );
    }

    @Test
    void logToSystemOutShouldOnlyLogToSystemOut()
    {
        /* The sent in filename should be ignored if logToSystemOut is used. */
        File targetFile = new File( dir.homeDir(), "debug.log" );

        ctx = LogConfig.createBuilder( targetFile.toPath(), Level.INFO )
                .logToSystemOut()
                .build();

        Logger logger = ctx.getLogger( "test" );
        logger.warn( "test" );

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( false );
        assertThat( suppressOutput.getOutputVoice().containsMessage( "test" ) ).isTrue();
    }

    @Test
    void reconfigureShouldUseNewSettings()
    {
        File targetFile = new File( dir.homeDir(), "debug.log" );
        File targetFile1 = new File( dir.homeDir(), "debug.log.1" );
        File targetFile2 = new File( dir.homeDir(), "debug.log.2" );
        File targetFile3 = new File( dir.homeDir(), "debug.log.3" );

        ctx = LogConfig.createBuilder( targetFile.toPath(), Level.INFO )
                .withRotation( 100, 2 )
                .build();

        Logger logger = ctx.getLogger( "test" );

        logger.warn( "test1" );
        logger.warn( "test2" );
        logger.warn( "test3" );
        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile1 ) ).isEqualTo( false );

        LogConfig.reconfigureLogging( ctx, LogConfig.createBuilder( targetFile.toPath(), Level.INFO ).withRotation( 10, 3 ) );

        /* Should now rotate on each message with the new limit. */
        logger.warn( "test4" );
        logger.warn( "test5" );
        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile1 ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile2 ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile3 ) ).isEqualTo( false );
    }

    @Test
    void withTimezoneUTC()
    {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        ctx = LogConfig.createBuilder( outContent, Level.INFO )
                .withTimezone( LogTimeZone.UTC )
                .build();

        Logger logger = ctx.getLogger( "org.neo4j.classname" );
        logger.warn( "test" );

        assertThat( outContent.toString() ).matches( format( DATE_PATTERN_NO_TIMEZONE + "\\+0000 %-5s \\[o.n.classname\\] test%n", Level.WARN ) );
    }

    @Test
    void standardFormatDefaults()
    {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        ctx = LogConfig.createBuilder( outContent, Level.INFO )
                .withFormat( FormattedLogFormat.STANDARD_FORMAT )
                .build();

        Logger logger = ctx.getLogger( "org.neo4j.classname" );
        logger.warn( "test" );

        assertThat( outContent.toString() ).matches( DATE_PATTERN + format( " %-5s \\[o.n.classname\\] test%n", Level.WARN ) );
    }

    @Test
    void standardFormatNoCategory()
    {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        ctx = LogConfig.createBuilder( outContent, Level.INFO )
                .withFormat( FormattedLogFormat.STANDARD_FORMAT )
                .withCategory( false )
                .build();

        Logger logger = ctx.getLogger( "org.neo4j.classname" );
        logger.warn( "test" );

        assertThat( outContent.toString() ).matches( DATE_PATTERN + format( " %-5s test%n", Level.WARN ) );
    }

    @Test
    void jsonFormatDefaults()
    {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        ctx = LogConfig.createBuilder( outContent, Level.INFO )
                .withFormat( FormattedLogFormat.JSON_FORMAT )
                .build();

        Logger logger = ctx.getLogger( "org.neo4j.classname" );
        logger.warn( "test" );

        assertThat( outContent.toString() ).matches( format(
                "\\{\"time\": \"" + DATE_PATTERN + "\", \"level\": \"%s\", \"category\": \"o.n.classname\", \"message\": \"test\"\\}%n", Level.WARN ) );
    }

    @Test
    void jsonFormatNoCategory()
    {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        ctx = LogConfig.createBuilder( outContent, Level.INFO )
                .withFormat( FormattedLogFormat.JSON_FORMAT )
                .withCategory( false )
                .build();

        Logger logger = ctx.getLogger( "org.neo4j.classname" );
        logger.warn( "test" );

        assertThat( outContent.toString() ).matches( format(
                "\\{\"time\": \"" + DATE_PATTERN + "\", \"level\": \"%s\", \"message\": \"test\"\\}%n", Level.WARN ) );
    }

    @Test
    void jsonFormatStacktrace()
    {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        ctx = LogConfig.createBuilder( outContent, Level.INFO )
                .withFormat( FormattedLogFormat.JSON_FORMAT )
                .build();

        Logger logger = ctx.getLogger( "org.neo4j.classname" );
        logger.warn( "test", newThrowable( "stack" ) );

        assertThat( outContent.toString() ).matches( format(
                "\\{\"time\": \"" + DATE_PATTERN +
                "\", \"level\": \"%s\", \"category\": \"o.n.classname\", \"message\": \"test\", \"stacktrace\": \" stack\"\\}%n", Level.WARN ) );
    }

    static Throwable newThrowable( final String stackTrace )
    {

        return new Throwable()
        {
            @Override
            public void printStackTrace( PrintWriter s )
            {
                s.append( stackTrace );
            }
        };
    }
}
