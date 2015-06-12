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
package org.neo4j.kernel.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.slf4j.Logger;
import org.slf4j.Marker;

import java.io.File;
import java.net.URL;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.RestartOnChange;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * Logging service that uses Logback as backend.
 */
public class LogbackService
        extends LifecycleAdapter
        implements Logging
{
    private final Config config;
    private final LoggerContext loggerContext;

    private final LifeSupport loggingLife = new LifeSupport();
    protected RestartOnChange restartOnChange;

    public LogbackService( Config config, LoggerContext loggerContext )
    {
        this(config, loggerContext, "neo4j-logback.xml", new Monitors());
    }

    public LogbackService( final Config config, final LoggerContext loggerContext,
                           final String logbackConfigurationFilename,
                           final Monitors monitors)
    {
        this.config = config;
        this.loggerContext = loggerContext;
        MonitoredRollingPolicy.setMonitorsInstance(monitors);

        // We do the initialization in the constructor, because some services will use logging during creation phase, before init.
        final File storeDir = config.get( GraphDatabaseSettings.store_dir );

        if ( storeDir != null )
        {
            File file = storeDir.getAbsoluteFile();
            if ( !file.exists() )
            {
                file.mkdirs();
            }

            File configuredInternalLog = config.get( GraphDatabaseSettings.internal_log_location );
            final File internalLog;
            if ( configuredInternalLog != null )
            {
                internalLog = configuredInternalLog;
                if ( !internalLog.getParentFile().exists() )
                {
                    internalLog.getParentFile().mkdirs();
                }
            }
            else
            {
                internalLog = new File( storeDir, StringLogger.DEFAULT_NAME );
            }

            // Neo4j specific log config
            loggingLife.add( new LifecycleAdapter()
            {
                @Override
                public void start()
                        throws Throwable
                {
                    JoranConfigurator configurator = new JoranConfigurator();
                    configurator.setContext( loggerContext );

                    if (config.getParams().containsKey( "ha.server_id" ))
                    {
                        loggerContext.putProperty( "host", config.getParams().get( "ha.server_id" ) );
                    }

                    loggerContext.putProperty( "neo_store", storeDir.getPath() );
                    loggerContext.putProperty( "internal_log", internalLog.getPath() );
                    loggerContext.putProperty( "remote_logging_enabled", config.get( GraphDatabaseSettings
                            .remote_logging_enabled ).toString() );
                    loggerContext.putProperty( "remote_logging_host", config.get( GraphDatabaseSettings
                            .remote_logging_host ) );
                    loggerContext.putProperty( "remote_logging_port", config.get( GraphDatabaseSettings
                            .remote_logging_port ).toString() );
                    try
                    {
                        URL resource = getClass().getClassLoader().getResource( logbackConfigurationFilename );

                        if (resource == null)
                        {
                            throw new IllegalStateException( String.format("Could not find %s configuration", logbackConfigurationFilename ));
                        }

                        configurator.doConfigure( resource );
                    }
                    catch ( JoranException e )
                    {
                        throw new IllegalStateException( "Failed to configure logging", e );
                    }
                }

                @Override
                public void stop()
                        throws Throwable
                {
                    loggerContext.getLogger( "org.neo4j" ).detachAndStopAllAppenders();
                }
            } );
            loggingLife.start();

            // Unset this to ensure we don't leak memory through statics
            MonitoredRollingPolicy.setMonitorsInstance(null);

            restartOnChange = new RestartOnChange( "remote_logging_", loggingLife );
            config.addConfigurationChangeListener( restartOnChange );
        }
    }

    @Override
    public void shutdown()
            throws Throwable
    {
        loggingLife.shutdown();
        if ( restartOnChange != null )
        {
            config.removeConfigurationChangeListener( restartOnChange );
        }
    }

    @Override
    public StringLogger getMessagesLog( Class loggingClass )
    {
        return new Slf4jToStringLoggerAdapter( loggerContext.getLogger( loggingClass ) );
    }

    @Override
    public ConsoleLogger getConsoleLog( Class loggingClass )
    {
        return new ConsoleLogger( new Slf4jToStringLoggerAdapter( loggerContext.getLogger( loggingClass ) ) );
    }

    private static class Slf4jToStringLoggerAdapter
            extends StringLogger
    {
        private final Logger logger;
        private final boolean debugEnabled;

        public Slf4jToStringLoggerAdapter( Logger logger )
        {
            this.logger = logger;
            debugEnabled = logger.isDebugEnabled();
        }

        @Override
        protected void logLine( String line )
        {
            logger.info( line );
        }

        @Override
        public void logLongMessage( final String msg, Visitor<LineLogger, RuntimeException> source, final boolean flush )
        {
            logMessage( msg, flush );
            source.visit( new LineLogger()
            {
                @Override
                public void logLine( String line )
                {
                    logMessage( line, flush );
                }
            } );
        }

        @Override
        public void logMessage( String msg, boolean flush )
        {
            if ( isDebugEnabled() )
            {
                logger.debug( msg );
            }
            else
            {
                logger.info( msg );
            }
        }

        @Override
        public void logMessage( String msg, LogMarker marker )
        {
           logger.info( from( marker ), msg );
        }

        @Override
        public void logMessage( String msg, Throwable cause, boolean flush )
        {
            logger.error( msg, cause );
        }

        @Override
        public void debug( String msg )
        {
            if ( isDebugEnabled() )
            {
                logger.debug( msg );
            }
        }

        @Override
        public void debug( String msg, Throwable cause )
        {
            logger.debug( msg, cause );
        }

        @Override
        public boolean isDebugEnabled()
        {
            return debugEnabled;
        }

        @Override
        public void info( String msg )
        {
            logger.info( msg );
        }

        @Override
        public void info( String msg, Throwable cause )
        {
            logger.info( msg, cause );
        }

        @Override
        public void warn( String msg )
        {
            logger.warn( msg );
        }

        @Override
        public void warn( String msg, Throwable throwable )
        {
            logger.warn( msg, throwable );
        }

        @Override
        public void error( String msg )
        {
            logger.error( msg );
        }

        @Override
        public void error( String msg, Throwable throwable )
        {
            logger.error( msg, throwable );
        }

        @Override
        public void addRotationListener( Runnable listener )
        {
            // Ignore
        }

        @Override
        public void flush()
        {
            // Ignore
        }

        @Override
        public void close()
        {
            // Ignore
        }

        private static Marker from( LogMarker marker )
        {
            return new Slf4jMarkerAdapter( marker.getName() );
        }
    }
}
