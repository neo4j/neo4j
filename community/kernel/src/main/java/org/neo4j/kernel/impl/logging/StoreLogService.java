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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import org.neo4j.concurrent.AsyncEventSender;
import org.neo4j.function.Consumer;
import org.neo4j.function.Consumers;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.Lifecycles;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;

import static org.neo4j.io.file.Files.createOrOpenAsOuputStream;

/**
 * {@link LogService} for a neo4j store, where internal log messages are directed to a {@code messages.log}
 * file and user log can be externally provided.
 */
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
        private final Map<String, Level> logLevels = new HashMap<>();
        private Level defaultLevel = Level.INFO;
        private Executor asyncExecutor;
        private Listener<Throwable> asyncErrorHandler = AsyncEventLogging.DEFAULT_ASYNC_ERROR_HANDLER;

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

        public Builder withLevel( String context, Level level )
        {
            this.logLevels.put( context, level );
            return this;
        }

        public Builder withDefaultLevel( Level defaultLevel )
        {
            this.defaultLevel = defaultLevel;
            return this;
        }

        /**
         * {@link LogProvider log providers} will be made asynchronous and will therefore let logging be unaffected
         * by I/O waits from flushing underlying buffer to disk.
         */
        public Builder withAsyncronousLogging( JobScheduler executor )
        {
            return withAsyncronousLogging( executor.executor( JobScheduler.Groups.logging ) );
        }

        /**
         * {@link LogProvider log providers} will be made asynchronous and will therefore let logging be unaffected
         * by I/O waits from flushing underlying buffer to disk.
         */
        public Builder withAsyncronousLogging( Executor executor )
        {
            this.asyncExecutor = executor;
            return this;
        }

        Builder withAsyncronousLogging( Executor executor, Listener<Throwable> errorHandler )
        {
            this.asyncExecutor = executor;
            this.asyncErrorHandler = errorHandler;
            return this;
        }

        public StoreLogService inStoreDirectory( FileSystemAbstraction fileSystem, File storeDir ) throws IOException
        {
            return toFile( fileSystem, new File( storeDir, INTERNAL_LOG_NAME ) );
        }

        public StoreLogService toFile( FileSystemAbstraction fileSystem, File internalLogPath ) throws IOException
        {
            return new StoreLogService( fileSystem, internalLogPath, this );
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

    private final LifeSupport internalLife = new LifeSupport();
    private final SimpleLogService logService;

    @SuppressWarnings( "resource" )
    private StoreLogService(
            FileSystemAbstraction fileSystem,
            File internalLog,
            final Builder builder ) throws IOException
    {
        if ( !internalLog.getParentFile().exists() )
        {
            internalLog.getParentFile().mkdirs();
        }

        final FormattedLogProvider.Builder internalLogBuilder = FormattedLogProvider.withUTCTimeZone()
                .withDefaultLogLevel( builder.defaultLevel ).withLogLevels( builder.logLevels );

        LogProvider internalLogProvider;
        Closeable closeInShutdown;
        if ( builder.internalLogRotationThreshold == 0 )
        {
            OutputStream outputStream = createOrOpenAsOuputStream( fileSystem, internalLog, true );
            internalLogProvider = internalLogBuilder.toOutputStream( outputStream );
            builder.rotationListener.accept( internalLogProvider );

            // We manage closing of the internal log since we create it in here. The user log is provided
            // to us (no pun intended) so we'll let the provider take care of closing that.
            closeInShutdown = outputStream;
        }
        else
        {
            RotatingFileOutputStreamSupplier rotatingSupplier = new RotatingFileOutputStreamSupplier(
                    fileSystem,
                    internalLog,
                    builder.internalLogRotationThreshold,
                    builder.internalLogRotationDelay,
                    builder.maxInternalLogArchives,
                    builder.rotationExecutor,
                    new RotatingFileOutputStreamSupplier.RotationListener()
            {
                @Override
                public void outputFileCreated( OutputStream newStream, OutputStream oldStream )
                {
                    FormattedLogProvider logProvider = internalLogBuilder.toOutputStream( newStream );
                    logProvider.getLog( StoreLogService.class ).info( "Opened new internal log file" );
                    builder.rotationListener.accept( logProvider );
                    logProvider.getLog( StoreLogService.class ).info( "Rotated internal log file" );
                }
            } );
            internalLogProvider = internalLogBuilder.toOutputStream( rotatingSupplier );
            closeInShutdown = rotatingSupplier;
        }
        internalLife.add( Lifecycles.close( closeInShutdown ) );

        // Configure the log providers to be asynchronous, if user wanted that behavior.
        LogProvider userLogProvider = builder.userLogProvider;
        if ( builder.asyncExecutor != null )
        {
            AsyncEventSender<AsyncLogEvent> events = internalLife.add(
                    new AsyncEventLogging( builder.asyncErrorHandler, builder.asyncExecutor ) );

            userLogProvider = new AsyncLogProvider( userLogProvider, events );
            internalLogProvider = new AsyncLogProvider( internalLogProvider, events );
        }

        this.logService = new SimpleLogService( userLogProvider, internalLogProvider );
    }

    @Override
    public void init() throws Throwable
    {
        internalLife.init();
    }

    @Override
    public void start() throws Throwable
    {
        internalLife.start();
    }

    @Override
    public void stop() throws Throwable
    {
        internalLife.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        internalLife.shutdown();
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
