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
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.OutputStreamAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.FormattedLogFormat;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogTimeZone;

import static org.neo4j.util.Preconditions.checkArgument;

public final class LogConfig
{
    private static final String APPENDER_NAME = "log";

    private LogConfig()
    {
    }

    public static void updateLogLevel( org.neo4j.logging.Level level, Neo4jLoggerContext context )
    {
        LoggerContext log4jContext = context.getLoggerContext();
        Configuration config = log4jContext.getConfiguration();
        Level newLevel = convertNeo4jLevelToLevel( level );

        LoggerConfig loggerConfig = config.getRootLogger();
        loggerConfig.setLevel( newLevel );

        // This causes all Loggers to refetch information from their LoggerConfig.
        log4jContext.updateLoggers();
    }

    public static void reconfigureLogging( Neo4jLoggerContext ctx, Builder builder )
    {
        checkArgument( !ctx.haveExternalResources(), "Can not reconfigure logging that is using output stream" );
        LoggerContext log4jContext = ctx.getLoggerContext();
        configureLogging( log4jContext, builder );
    }

    public static Builder createBuilder( FileSystemAbstraction fs, Path logPath, org.neo4j.logging.Level level )
    {
        return new Builder( fs, logPath, level );
    }

    public static Builder createBuilder( OutputStream outputStream, org.neo4j.logging.Level level )
    {
        return new Builder( outputStream, level );
    }

    private static void configureLogging( LoggerContext context, Builder builder )
    {
        Configuration configuration = new DefaultConfiguration()
        {
            @Override
            protected void setToDefault()
            {
                // no defaults
            }
        };

        Neo4jLogLayout layout = getLayout( builder );

        Appender appender = getAppender( builder, layout );
        appender.start();
        configuration.addAppender( appender );

        // Must be done after the appender is started to get first file without header - which is the
        // desired behavior since diagnostics (the usual header) is written when starting dbms.
        if ( builder.headerLogger != null )
        {
            layout.setHeaderLogger( builder.headerLogger, builder.headerClassName );
        }

        LoggerConfig rootLogger = configuration.getRootLogger();
        rootLogger.addAppender( appender, null, null );
        rootLogger.setLevel( builder.level );

        context.setConfiguration( configuration );
    }

    private static Neo4jLogLayout getLayout( Builder builder )
    {
        String datePattern = "yyyy-MM-dd HH:mm:ss.SSSZ";
        if ( builder.format == FormattedLogFormat.STANDARD_FORMAT )
        {
            String date = "%d{" + datePattern + "}" + (builder.timezone == LogTimeZone.UTC ? "{GMT+0}" : "");
            return Neo4jLogLayout.createLayout( builder.includeCategory ?
                    date + " %-5p [%c{1.}] %m%n" :
                    date + " %-5p %m%n" );
        }
        return Neo4jJsonLogLayout.createLayout( datePattern, builder.timezone == LogTimeZone.UTC ? "GMT+0" : null, builder.includeCategory );
    }

    private static Appender getAppender( Builder builder, Layout<String> layout )
    {
        OutputStream outputStream = builder.outputStream;

        if ( builder.logToSystemOut )
        {
            return ConsoleAppender.newBuilder()
                    .setName( APPENDER_NAME )
                    .setLayout( layout )
                    .setTarget( ConsoleAppender.Target.SYSTEM_OUT )
                    .build();
        }
        else if ( builder.fileSystemAbstraction instanceof DefaultFileSystemAbstraction )
        {
            // Uses RollingFile appender even if no rotation is requested (but with threshold that won't be reached) to be able to
            // reconfigure between with and without rotation.
            return createRollingFileAppender( builder, layout );
        }
        return ((OutputStreamAppender.Builder<?>) OutputStreamAppender.newBuilder().setName( APPENDER_NAME ).setLayout( layout ))
                .setTarget( outputStream ).build();
    }

    private static Appender createRollingFileAppender( Builder builder, Layout<String> layout )
    {
        long rotationThreshold = builder.rotationThreshold;
        int maxArchives = builder.maxArchives;

        if ( builder.rotationThreshold == 0 || builder.maxArchives == 0 )
        {
            // Should not rotate - set threshold that won't be reached.
            rotationThreshold = Long.MAX_VALUE;
            maxArchives = 1;
        }

        SizeBasedTriggeringPolicy policy = SizeBasedTriggeringPolicy.createPolicy( String.valueOf( rotationThreshold ) );

        DefaultRolloverStrategy rolloverStrategy =
                DefaultRolloverStrategy.newBuilder().withMax( String.valueOf( maxArchives ) ).withFileIndex( "min" ).build();

        return RollingFileAppender.newBuilder()
                .setName( APPENDER_NAME )
                .setLayout( layout )
                .withCreateOnDemand( builder.createOnDemand )
                .withFileName( builder.logPath.toString() )
                .withFilePattern( builder.logPath + ".%i" )
                .withPolicy( policy )
                .withStrategy( rolloverStrategy )
                .build();
    }

    private static Level convertNeo4jLevelToLevel( org.neo4j.logging.Level level )
    {
        switch ( level )
        {
        case ERROR:
            return Level.ERROR;
        case WARN:
            return Level.WARN;
        case INFO:
            return Level.INFO;
        case DEBUG:
        default:
            return Level.DEBUG;
        }
    }

    public static class Builder
    {
        private final Path logPath;
        private final Level level;
        private OutputStream outputStream;
        private long rotationThreshold;
        private int maxArchives;
        private FormattedLogFormat format = FormattedLogFormat.STANDARD_FORMAT;
        private LogTimeZone timezone = LogTimeZone.UTC;
        private boolean includeCategory = true;
        private Consumer<Log> headerLogger;
        private String headerClassName;
        private boolean logToSystemOut;
        private boolean createOnDemand;
        private FileSystemAbstraction fileSystemAbstraction;

        private Builder( FileSystemAbstraction fileSystemAbstraction, Path logPath, org.neo4j.logging.Level level )
        {
            this.fileSystemAbstraction = fileSystemAbstraction;
            this.logPath = logPath;
            this.outputStream = null;
            this.level = convertNeo4jLevelToLevel( level );
        }

        private Builder( OutputStream outputStream, org.neo4j.logging.Level level )
        {
            this.logPath = null;
            this.outputStream = outputStream;
            this.level = convertNeo4jLevelToLevel( level );
        }

        public Builder withRotation( long rotationThreshold, int maxArchives )
        {
            this.rotationThreshold = rotationThreshold;
            this.maxArchives = maxArchives;
            return this;
        }

        public Builder withTimezone( LogTimeZone timezone )
        {
            this.timezone = timezone;
            return this;
        }

        public Builder withFormat( FormattedLogFormat format )
        {
            this.format = format;
            return this;
        }

        public Builder withCategory( boolean includeCategory )
        {
            this.includeCategory = includeCategory;
            return this;
        }

        public Builder withHeaderLogger( Consumer<Log> headerLogger, String headerClassName )
        {
            this.headerLogger = headerLogger;
            this.headerClassName = headerClassName;
            return this;
        }

        public Builder logToSystemOut()
        {
            this.logToSystemOut = true;
            return this;
        }

        public Builder createOnDemand()
        {
            this.createOnDemand = true;
            return this;
        }

        public Neo4jLoggerContext build()
        {
            try
            {
                LoggerContext context = new LoggerContext( "loggercontext" );

                // We only need to have a rotating file supplier for the real file system
                if ( fileSystemAbstraction instanceof DefaultFileSystemAbstraction )
                {
                    if ( outputStream != null )
                    {
                        throw new IllegalStateException( "When using filesystem abstraction you cannot provide a stream since we cant rotate that" );
                    }
                    configureLogging( context, this );
                    return new Neo4jLoggerContext( context, null );
                }

                // Everything else should be fine without rotation
                if ( outputStream == null )
                {
                    // We are use a different file system than DefaultFileSystemAbstraction, we cannot use log4j file appenders here
                    fileSystemAbstraction.mkdirs( logPath.getParent() );
                    outputStream = fileSystemAbstraction.openAsOutputStream( logPath, true );
                    configureLogging( context, this );
                    return new Neo4jLoggerContext( context, outputStream );
                }
                else
                {
                    configureLogging( context, this );
                    return new Neo4jLoggerContext( context, null );
                }
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }
}
