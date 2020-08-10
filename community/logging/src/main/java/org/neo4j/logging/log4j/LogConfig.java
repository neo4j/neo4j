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

import java.io.OutputStream;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.neo4j.logging.FormattedLogFormat;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogTimeZone;

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
        LoggerContext log4jContext = ctx.getLoggerContext();
        configureLogging( log4jContext, builder );
    }

    public static Builder createBuilder( Path logPath, org.neo4j.logging.Level level )
    {
        return new Builder( logPath, level );
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
        if ( builder.format == FormattedLogFormat.STANDARD_FORMAT )
        {
            return Neo4jLogLayout.createLayout( builder.includeCategory ?
                                                getDateFormat( builder ) + " %-5p [%c{1.}] %m%n" :
                                                getDateFormat( builder ) + " %-5p %m%n" );
        }
        return Neo4jJsonLogLayout.createLayout( getDateFormat( builder ), builder.includeCategory );
    }

    private static Appender getAppender( Builder builder, Layout<String> layout )
    {
        if ( builder.logToSystemOut )
        {
            return ConsoleAppender.newBuilder()
                    .setName( APPENDER_NAME )
                    .setLayout( layout )
                    .setTarget( ConsoleAppender.Target.SYSTEM_OUT )
                    .build();
        }
        else if ( builder.logPath != null )
        {
            // Uses RollingFile appender even if no rotation is requested (but with threshold that won't be reached) to be able to
            // reconfigure between with and without rotation.
            return createRollingFileAppender( builder, layout );
        }
        else
        {
            return ((OutputStreamAppender.Builder<?>) OutputStreamAppender.newBuilder().setName( APPENDER_NAME ).setLayout( layout ))
                    .setTarget( builder.outputStream ).build();
        }
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

    private static String getDateFormat( Builder builder )
    {
        if ( builder.timezone == LogTimeZone.UTC )
        {
            return "%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0}";
        }
        return "%d{yyyy-MM-dd HH:mm:ss.SSSZ}";
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
        private final OutputStream outputStream;
        private final Level level;
        private long rotationThreshold;
        private int maxArchives;
        private FormattedLogFormat format = FormattedLogFormat.STANDARD_FORMAT;
        private LogTimeZone timezone = LogTimeZone.UTC;
        private boolean includeCategory = true;
        private Consumer<Log> headerLogger;
        private String headerClassName;
        private boolean logToSystemOut;
        private boolean createOnDemand;

        private Builder( Path logPath, org.neo4j.logging.Level level )
        {
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
            LoggerContext context = new LoggerContext( "loggercontext" );
            configureLogging( context, this );
            return new Neo4jLoggerContext( context );
        }
    }
}
