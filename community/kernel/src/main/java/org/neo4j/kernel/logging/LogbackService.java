/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.io.File;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.RestartOnChange;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

/**
 * Logging service that uses Logback as backend.
 */
public class LogbackService
    extends LifecycleAdapter
    implements Logging
{
    private Config config;
    private LoggerContext loggerContext;

    private LifeSupport loggingLife = new LifeSupport();
    protected RestartOnChange restartOnChange;

    public LogbackService( Config config )
    {
        this.config = config;
    }

    @Override
    public void init()
        throws Throwable
    {
        final String storeDir = config.get( InternalAbstractGraphDatabase.Configuration.store_dir );

        File file = new File( storeDir ).getAbsoluteFile();
        if (!file.exists())
            file.mkdirs();

        loggerContext = (LoggerContext) StaticLoggerBinder.getSingleton().getLoggerFactory();

        // Neo4j specific log config
        loggingLife.add( new LifecycleAdapter()
        {
            @Override
            public void start()
                throws Throwable
            {
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext( loggerContext );
                loggerContext.putProperty( "neo_store", storeDir );
                loggerContext.putProperty( "remote_logging_enabled", config.get( GraphDatabaseSettings.remote_logging_enabled ).toString() );
                loggerContext.putProperty( "remote_logging_host", config.get( GraphDatabaseSettings.remote_logging_host ) );
                loggerContext.putProperty( "remote_logging_port", config.get( GraphDatabaseSettings.remote_logging_port ).toString() );
                try
                {
                    configurator.doConfigure( getClass().getResource( "/neo4j-logback.xml" ) );
                }
                catch( JoranException e )
                {
                    throw new IllegalStateException("Failed to configure logging", e );
                }
            }

            @Override
            public void stop()
                throws Throwable
            {
                loggerContext.getLogger( Loggers.NEO4J ).detachAndStopAllAppenders();
            }
        });
        loggingLife.start();

        restartOnChange = new RestartOnChange( "remote_logging_", loggingLife );
        config.addConfigurationChangeListener( restartOnChange );
    }

    @Override
    public void shutdown()
        throws Throwable
    {
        loggingLife.shutdown();
        config.removeConfigurationChangeListener( restartOnChange );
    }

    @Override
    public StringLogger getLogger( String name )
    {
        return new Slf4jStringLogger( LoggerFactory.getLogger( name ));
    }

    public static class Slf4jStringLogger
        extends StringLogger
    {
        Logger logger;

        Slf4jStringLogger( Logger logger )
        {
            this.logger = logger;
        }

        @Override
        protected void logLine( String line )
        {
            logger.info( line );
        }

        @Override
        public void logLongMessage( final String msg, Visitor<LineLogger> source, final boolean flush )
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
            if (logger.isDebugEnabled())
                logger.debug( msg );
            else
                logger.info( msg );
        }

        @Override
        public void logMessage( String msg, Throwable cause, boolean flush )
        {
            logger.error( msg, cause );
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
    }
}
