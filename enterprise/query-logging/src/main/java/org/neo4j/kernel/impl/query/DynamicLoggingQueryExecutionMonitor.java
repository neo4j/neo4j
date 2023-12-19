/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.query;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneId;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.io.file.Files.createOrOpenAsOutputStream;
import static org.neo4j.kernel.impl.query.QueryLogger.NO_LOG;

class DynamicLoggingQueryExecutionMonitor extends LifecycleAdapter implements QueryExecutionMonitor
{
    private final Config config;
    private final FileSystemAbstraction fileSystem;
    private final JobScheduler scheduler;
    private final Log debugLog;

    /**
     * The currently configured QueryLogger.
     * This may be accessed concurrently by any thread, even while the logger is being reconfigured.
     */
    private volatile QueryLogger currentLog = NO_LOG;

    // These fields are only accessed during (re-) configuration, and are protected from concurrent access
    // by the monitor lock on DynamicQueryLogger.
    private ZoneId currentLogTimeZone;
    private FormattedLog.Builder logBuilder;
    private File currentQueryLogFile;
    private long currentRotationThreshold;
    private int currentMaxArchives;
    private Log log;
    private Closeable closable;

    DynamicLoggingQueryExecutionMonitor( Config config, FileSystemAbstraction fileSystem, JobScheduler scheduler,
                                         Log debugLog )
    {
        this.config = config;
        this.fileSystem = fileSystem;
        this.scheduler = scheduler;
        this.debugLog = debugLog;
    }

    @Override
    public synchronized void init()
    {
        // This set of settings are currently not dynamic:
        currentLogTimeZone = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
        logBuilder = FormattedLog.withZoneId( currentLogTimeZone );
        currentQueryLogFile = config.get( GraphDatabaseSettings.log_queries_filename );

        updateSettings();

        registerDynamicSettingUpdater( GraphDatabaseSettings.log_queries );
        registerDynamicSettingUpdater( GraphDatabaseSettings.log_queries_threshold );
        registerDynamicSettingUpdater( GraphDatabaseSettings.log_queries_rotation_threshold );
        registerDynamicSettingUpdater( GraphDatabaseSettings.log_queries_max_archives );
    }

    private <T> void registerDynamicSettingUpdater( Setting<T> setting )
    {
        config.registerDynamicUpdateListener( setting, ( a,b ) -> updateSettings() );
    }

    private synchronized void updateSettings()
    {
        updateLogSettings();
        updateQueryLoggerSettings();
    }

    private void updateQueryLoggerSettings()
    {
        // This method depends on any log settings having been updated before hand, via updateLogSettings.
        // The only dynamic settings here are log_queries, and log_queries_threshold which is read by the
        // ConfiguredQueryLogger constructor. We can add more in the future, though. The various content settings
        // are prime candidates.
        if ( config.get( GraphDatabaseSettings.log_queries ) )
        {
            currentLog = new ConfiguredQueryLogger( log, config );
        }
        else
        {
            currentLog = NO_LOG;
        }
    }

    private void updateLogSettings()
    {
        // The dynamic setting here is log_queries, log_queries_rotation_threshold, and log_queries_max_archives.
        // NOTE: We can't register this method as a settings update callback, because we don't update the `currentLog`
        // field in this method. Settings updates must always go via the `updateQueryLoggerSettings` method.
        if ( config.get( GraphDatabaseSettings.log_queries ) )
        {
            long rotationThreshold = config.get( GraphDatabaseSettings.log_queries_rotation_threshold );
            int maxArchives = config.get( GraphDatabaseSettings.log_queries_max_archives );

            try
            {
                if ( logRotationIsEnabled( rotationThreshold ) )
                {
                    boolean needsRebuild = closable == null; // We need to rebuild the log if we currently don't have any,
                    needsRebuild |= currentRotationThreshold != rotationThreshold; // or if rotation threshold has changed,
                    needsRebuild |= currentMaxArchives != maxArchives; // or if the max archives setting has changed.
                    if ( needsRebuild )
                    {
                        closeCurrentLogIfAny();
                        buildRotatingLog( rotationThreshold, maxArchives );
                    }
                }
                else if ( currentRotationThreshold != rotationThreshold || closable == null )
                {
                    // We go from rotating (or uninitialised) log to non-rotating. Always rebuild.
                    closeCurrentLogIfAny();
                    buildNonRotatingLog();
                }

                currentRotationThreshold = rotationThreshold;
                currentMaxArchives = maxArchives;
            }
            catch ( IOException exception )
            {
                debugLog.warn( "Failed to build query log", exception );
            }
        }
        else
        {
            closeCurrentLogIfAny();
        }
    }

    private boolean logRotationIsEnabled( long threshold )
    {
        return threshold > 0;
    }

    private void closeCurrentLogIfAny()
    {
        if ( closable != null )
        {
            try
            {
                closable.close();
            }
            catch ( IOException exception )
            {
                debugLog.warn( "Failed to close current log: " + closable, exception );
            }
            closable = null;
        }
    }

    private void buildRotatingLog( long rotationThreshold, int maxArchives ) throws IOException
    {
        RotatingFileOutputStreamSupplier rotatingSupplier = new RotatingFileOutputStreamSupplier(
                fileSystem, currentQueryLogFile,
                rotationThreshold, 0, maxArchives,
                scheduler.executor( JobScheduler.Groups.queryLogRotation ) );
        log = logBuilder.toOutputStream( rotatingSupplier );
        closable = rotatingSupplier;
    }

    private void buildNonRotatingLog() throws IOException
    {
        OutputStream logOutputStream = createOrOpenAsOutputStream( fileSystem, currentQueryLogFile, true );
        log = logBuilder.toOutputStream( logOutputStream );
        closable = logOutputStream;
    }

    @Override
    public synchronized void shutdown()
    {
        closeCurrentLogIfAny();
    }

    @Override
    public void startQueryExecution( ExecutingQuery query )
    {
    }

    @Override
    public void endFailure( ExecutingQuery query, Throwable failure )
    {
        currentLog.failure( query, failure );
    }

    @Override
    public void endSuccess( ExecutingQuery query )
    {
        currentLog.success( query );
    }
}
