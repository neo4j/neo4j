/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.query;

import java.io.File;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.query.QuerySession.MetadataKey;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.impl.util.StringLogger.DEFAULT_THRESHOLD_FOR_ROTATION;

@Service.Implementation( KernelExtensionFactory.class )
public class QueryLoggerKernelExtension extends KernelExtensionFactory<QueryLoggerKernelExtension.Dependencies>
{
    public interface Dependencies
    {
        FileSystemAbstraction filesystem();

        Config config();

        Monitors monitoring();

        StringLogger logger();
    }

    public QueryLoggerKernelExtension()
    {
        super( "query-logging" );
    }

    @Override
    public Lifecycle newKernelExtension( Dependencies the ) throws Throwable
    {
        Config config = the.config();
        if ( config.get( GraphDatabaseSettings.log_queries ) )
        {
            File logFilename = config.get( GraphDatabaseSettings.log_queries_filename );
            if ( logFilename == null)
            {
                the.logger().warn( GraphDatabaseSettings.log_queries.name() +
                                   " is enabled but no " +
                                   GraphDatabaseSettings.log_queries_filename.name() +
                                   " has not been provided in configuration, hence query logging is suppressed" );
                return null;
            }

            long thresholdMillis = config.get( GraphDatabaseSettings.log_queries_threshold );
            LoggerFactory loggerFactory = new LoggerFactory( the.filesystem(), logFilename );
            QueryLogger logger = new QueryLogger( Clock.SYSTEM_CLOCK, loggerFactory, thresholdMillis );
            the.monitoring().addMonitorListener( logger );
            return logger;
        }
        return null;
    }

    public static class QueryLogger extends LifecycleAdapter implements QueryExecutionMonitor
    {
        private static final MetadataKey<Long> START_TIME = new MetadataKey<>( Long.class, "start time" );
        private static final MetadataKey<String> QUERY_STRING = new MetadataKey<>( String.class, "query string" );
        private final Clock clock;
        private final Factory<StringLogger> loggerFactory;
        private final long thresholdMillis;
        private StringLogger logger;

        public QueryLogger( Clock clock, Factory<StringLogger> loggerFactory, long thresholdMillis )
        {
            this.clock = clock;
            this.loggerFactory = loggerFactory;
            this.thresholdMillis = thresholdMillis;
        }

        @Override
        public void init()
        {
            logger = loggerFactory.newInstance();
        }

        @Override
        public void shutdown()
        {
            logger.close();
            logger = null;
        }

        @Override
        public void startQueryExecution( QuerySession session, String query )
        {
            long startTime = clock.currentTimeMillis();
            Object oldTime = session.put( START_TIME, startTime );
            Object oldQuery = session.put( QUERY_STRING, query );
            if ( oldTime != null || oldQuery != null )
            {
                logger.warn( String.format( "Concurrent queries for session %s: \"%s\" @ %s and \"%s\" @ %s",
                        session, oldQuery, oldTime, query, startTime ) );
            }
        }

        @Override
        public void endFailure( QuerySession session, Throwable failure )
        {
            String query = session.remove( QUERY_STRING );
            Long startTime = session.remove( START_TIME );
            if ( startTime != null )
            {
                long time = clock.currentTimeMillis() - startTime;
                logger.logMessage( String.format( "FAILURE %d ms: %s - %s", time, session,
                        query == null ? "<unknown query>" : query ), failure );
            }
        }

        @Override
        public void endSuccess( QuerySession session )
        {
            String query = session.remove( QUERY_STRING );
            Long startTime = session.remove( START_TIME );
            if ( startTime != null )
            {
                long time = clock.currentTimeMillis() - startTime;
                if ( time >= thresholdMillis )
                {
                    logger.logMessage( String.format( "SUCCESS %d ms: %s - %s", time, session,
                            query == null ? "<unknown query>" : query ) );
                }
            }
        }
    }

    private static class LoggerFactory implements Factory<StringLogger>
    {
        private final FileSystemAbstraction filesystem;
        private final File logfile;

        public LoggerFactory( FileSystemAbstraction filesystem, File logfile )
        {
            this.filesystem = filesystem;
            this.logfile = logfile;
        }

        @Override
        public StringLogger newInstance()
        {
            return StringLogger.logger( filesystem, logfile, DEFAULT_THRESHOLD_FOR_ROTATION, false );
        }
    }
}
