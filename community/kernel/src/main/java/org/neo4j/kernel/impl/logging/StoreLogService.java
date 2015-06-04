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
package org.neo4j.kernel.impl.logging;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executor;

import org.neo4j.function.Consumer;
import org.neo4j.function.Consumers;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;

import static org.neo4j.io.file.Files.createOrOpenAsOuputStream;

public class StoreLogService extends AbstractLogService implements Lifecycle
{
    public static final String INTERNAL_LOG_NAME = "messages.log";

    private final Closeable closeable;
    private final SimpleLogService logService;

    public StoreLogService( LogProvider userLogProvider, FileSystemAbstraction fileSystem, File storeDir, Config config, JobScheduler jobScheduler ) throws IOException
    {
        this( userLogProvider, fileSystem, storeDir, config, 0L, 0, 0, jobScheduler, Consumers.<LogProvider>noop() );
    }

    public StoreLogService( LogProvider userLogProvider, FileSystemAbstraction fileSystem, File storeDir, Config config,
                            long internalLogRotationThreshold, int internalLogRotationDelay, int maxInternalLogArchives,
                            JobScheduler jobScheduler, final Consumer<LogProvider> rotationListener ) throws IOException
    {
        this( userLogProvider, fileSystem, storeDir, config, internalLogRotationThreshold, internalLogRotationDelay, maxInternalLogArchives,
                jobScheduler.executor( JobScheduler.Groups.internalLogRotation ), rotationListener );
    }

    public StoreLogService( LogProvider userLogProvider, FileSystemAbstraction fileSystem, File storeDir, Config config,
                            long internalLogRotationThreshold, int internalLogRotationDelay, int maxInternalLogArchives,
                            Executor rotationExecutor, final Consumer<LogProvider> rotationListener ) throws IOException
    {
        File internalLogLocation = config.get( GraphDatabaseSettings.internal_log_location );
        final File logFile;
        if ( internalLogLocation != null )
        {
            logFile = internalLogLocation;
            if ( !logFile.getParentFile().exists() )
            {
                logFile.getParentFile().mkdirs();
            }
        }
        else
        {
            logFile = new File( storeDir, StoreLogService.INTERNAL_LOG_NAME );
        }

        FormattedLogProvider internalLogProvider;
        if ( internalLogRotationThreshold == 0 )
        {
            OutputStream outputStream = createOrOpenAsOuputStream( fileSystem, logFile, true );
            internalLogProvider = FormattedLogProvider.withUTCTimeZone().toOutputStream( outputStream );
            rotationListener.accept( internalLogProvider );
            this.closeable = outputStream;
        } else
        {
            RotatingFileOutputStreamSupplier rotatingSupplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile,
                    internalLogRotationThreshold, internalLogRotationDelay, maxInternalLogArchives,
                    rotationExecutor, new RotatingFileOutputStreamSupplier.RotationListener()
            {
                @Override
                public void outputFileCreated( OutputStream newStream, OutputStream oldStream )
                {
                    FormattedLogProvider logProvider = FormattedLogProvider.withUTCTimeZone().toOutputStream( newStream );
                    logProvider.getLog( StoreLogService.class ).info( "Opened new internal log file" );
                    rotationListener.accept( logProvider );
                    logProvider.getLog( StoreLogService.class ).info( "Rotated internal log file" );
                }
            } );
            internalLogProvider = FormattedLogProvider.withUTCTimeZone().toOutputStream( rotatingSupplier );
            this.closeable = rotatingSupplier;
        }
        this.logService = new SimpleLogService( userLogProvider, internalLogProvider );
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
        closeable.close();
    }

    @Override
    public LogProvider getUserLogProvider()
    {
        return logService.getUserLogProvider();
    }

    @Override
    public LogProvider getInternalLogProvider()
    {
        return logService.getInternalLogProvider();
    }
}
