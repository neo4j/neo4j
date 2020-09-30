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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.OutputStreamAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * Should be used for files where rotation is required and the messages needs no additional formatting by the logging framework.
 */
public class RotatingLogFileWriter implements Closeable
{
    private static final String APPENDER_NAME = "log";

    private final Logger log;
    private final Neo4jLoggerContext ctx;

    /**
     * @param fs The filesystem abstraction. Rotating files will only be created for DefaultFileSystemAbstraction, for other abstractions no rotation is done.
     * @param logPath Path of the log file.
     * @param rotationThreshold The threshold to rotate on in bytes.
     * @param maxArchives The maximum number of archive files to keep.
     * @param fileSuffix File suffix of the archive files. If the file suffix ends with '.gz' or '.zip' the resulting archive will be compressed using
     *                   the compression scheme that matches the suffix. Empty string if no additional file suffix should be added.
     * @param header String to print at beginning of each new file. Note that the header has no implicit newline so that must be added in the string if desired.
     */
    public RotatingLogFileWriter( FileSystemAbstraction fs, Path logPath, long rotationThreshold, int maxArchives, String fileSuffix, String header )
    {
        ctx = setupLogFile( fs, logPath, rotationThreshold, maxArchives, fileSuffix, header );
        log = ctx.getLogger( "" );
    }

    public void printf( String pattern, Object... params )
    {
        log.printf( Level.DEBUG, pattern, params );
    }

    @Override
    public void close() throws IOException
    {
        ctx.close();
    }

    private Neo4jLoggerContext setupLogFile( FileSystemAbstraction fileSystemAbstraction, Path logPath, long rotationThreshold, int maxArchives,
            String fileSuffix, String header )
    {
        try
        {
            Closeable additionalCloseable = null;
            Configuration configuration = new DefaultConfiguration()
            {
                @Override
                protected void setToDefault()
                {
                    // no defaults
                }
            };

            // Just adds a header to the beginning of each file - no transformation will be done on the log messages.
            PatternLayout layout = PatternLayout.newBuilder().withConfiguration( configuration ).withHeader( header ).build();

            Appender appender;
            if ( fileSystemAbstraction instanceof DefaultFileSystemAbstraction )
            {
                appender = RollingFileAppender.newBuilder()
                        .setName( APPENDER_NAME )
                        .setLayout( layout )
                        .withFileName( logPath.toString() )
                        .withFilePattern( logPath + ".%i" + fileSuffix )
                        .withPolicy( SizeBasedTriggeringPolicy.createPolicy( String.valueOf( rotationThreshold ) ) )
                        .withStrategy( DefaultRolloverStrategy.newBuilder().withMax( String.valueOf( maxArchives ) ).withFileIndex( "min" ).build() )
                        .build();
            }
            else
            {
                // When using a different file system than DefaultFileSystemAbstraction for tests, we cannot use log4j file appenders since
                // it will create files directly in the real filesystem ignoring our abstraction.
                fileSystemAbstraction.mkdirs( logPath.getParent() );
                OutputStream outputStream = fileSystemAbstraction.openAsOutputStream( logPath, true );
                additionalCloseable = outputStream;
                appender = ((OutputStreamAppender.Builder<?>) OutputStreamAppender.newBuilder().setName( APPENDER_NAME ).setLayout( layout ))
                        .setTarget( outputStream ).build();
            }
            appender.start();
            configuration.addAppender( appender );

            LoggerConfig rootLogger = configuration.getRootLogger();
            rootLogger.addAppender( appender, null, null );
            rootLogger.setLevel( Level.DEBUG );

            LoggerContext context = new LoggerContext( "loggercontext" );
            context.setConfiguration( configuration );
            return new Neo4jLoggerContext( context, additionalCloseable );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
