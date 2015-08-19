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
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;

import static org.neo4j.io.file.Files.createOrOpenAsOuputStream;

public class StoreLogService extends AbstractLogService implements Lifecycle
{
    public static final String INTERNAL_LOG_NAME = "messages.log";

    public static class Builder
    {
        private LogProvider userLogProvider = NullLogProvider.getInstance();
        private Executor rotationExecutor;
        private long internalLogRotationThreshold = 0L;
        private int internalLogRotationDelay = 0;
        private int maxInternalLogArchives = 0;
        private Consumer<LogProvider> rotationListener = Consumers.noop();
        private Level level = Level.INFO;

        private Builder()
        {
        }

        public Builder withUserLogProvider( LogProvider userLogProvider )
        {
            this.userLogProvider = userLogProvider;
            return this;
        }

        public Builder withRotation( long internalLogRotationThreshold, int internalLogRotationDelay, int maxInternalLogArchives,
                JobScheduler jobScheduler )
        {
            return withRotation( internalLogRotationThreshold, internalLogRotationDelay, maxInternalLogArchives,
                    jobScheduler.executor( JobScheduler.Groups.internalLogRotation ) );
        }

        public Builder withRotation( long internalLogRotationThreshold, int internalLogRotationDelay, int maxInternalLogArchives,
                Executor rotationExecutor )
        {
            this.internalLogRotationThreshold = internalLogRotationThreshold;
            this.internalLogRotationDelay = internalLogRotationDelay;
            this.maxInternalLogArchives = maxInternalLogArchives;
            this.rotationExecutor = rotationExecutor;
            return this;
        }

        public Builder withRotationListener( Consumer<LogProvider> rotationListener )
        {
            this.rotationListener = rotationListener;
            return this;
        }

        public Builder withLevel( Level level )
        {
            this.level = level;
            return this;
        }

        public StoreLogService inStoreDirectory( FileSystemAbstraction fileSystem, File storeDir ) throws IOException
        {
            return toFile( fileSystem, new File( storeDir, INTERNAL_LOG_NAME ) );
        }

        public StoreLogService toFile( FileSystemAbstraction fileSystem, File internalLogPath ) throws IOException
        {
            return new StoreLogService(
                    userLogProvider,
                    fileSystem, internalLogPath, level,
                    internalLogRotationThreshold, internalLogRotationDelay, maxInternalLogArchives, rotationExecutor, rotationListener );
        }
    }

    public static Builder withUserLogProvider( LogProvider userLogProvider )
    {
        return new Builder().withUserLogProvider( userLogProvider );
    }

    public static Builder withRotation( long internalLogRotationThreshold, int internalLogRotationDelay, int maxInternalLogArchives, JobScheduler jobScheduler )
    {
        return new Builder().withRotation( internalLogRotationThreshold, internalLogRotationDelay, maxInternalLogArchives, jobScheduler );
    }

    public static StoreLogService inStoreDirectory( FileSystemAbstraction fileSystem, File storeDir ) throws IOException
    {
        return new Builder().inStoreDirectory( fileSystem, storeDir );
    }

    private final Closeable closeable;
    private final SimpleLogService logService;

    private StoreLogService( LogProvider userLogProvider,
            FileSystemAbstraction fileSystem,
            File internalLog,
            Level level,
            long internalLogRotationThreshold,
            int internalLogRotationDelay,
            int maxInternalLogArchives,
            Executor rotationExecutor,
            final Consumer<LogProvider> rotationListener ) throws IOException
    {
        if ( !internalLog.getParentFile().exists() )
        {
            internalLog.getParentFile().mkdirs();
        }

        FormattedLogProvider internalLogProvider;
        if ( internalLogRotationThreshold == 0 )
        {
            OutputStream outputStream = createOrOpenAsOuputStream( fileSystem, internalLog, true );
            internalLogProvider = FormattedLogProvider.withUTCTimeZone().toOutputStream( outputStream );
            rotationListener.accept( internalLogProvider );
            this.closeable = outputStream;
        }
        else
        {
            RotatingFileOutputStreamSupplier rotatingSupplier = new RotatingFileOutputStreamSupplier( fileSystem, internalLog,
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
            internalLogProvider = FormattedLogProvider.withUTCTimeZone().withLogLevel( level ).toOutputStream(
                    rotatingSupplier );
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
