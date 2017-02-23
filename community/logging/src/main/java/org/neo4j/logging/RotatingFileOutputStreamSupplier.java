/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.logging;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;

import static org.neo4j.io.file.Files.createOrOpenAsOuputStream;

/**
 * A {@link Supplier} of {@link OutputStream}s backed by on-disk files, which
 * are rotated and archived when a specified size is reached. The {@link #get()} method
 * will always return an OutputStream to the current output file without directly performing
 * any IO or blocking, and, when necessary, will trigger rotation via the {@link Executor}
 * supplied during construction.
 */
public class RotatingFileOutputStreamSupplier implements Supplier<OutputStream>, Closeable
{
    /**
     * A listener for the rotation process
     */
    public static class RotationListener
    {
        public void outputFileCreated( OutputStream out )
        {
        }

        public void rotationCompleted( OutputStream out )
        {
        }

        public void rotationError( Exception e, OutputStream out )
        {
        }
    }

    private static final LongSupplier DEFAULT_CURRENT_TIME_SUPPLIER = System::currentTimeMillis;

    private final LongSupplier currentTimeSupplier;
    private final FileSystemAbstraction fileSystem;
    private final File outputFile;
    private final long rotationThresholdBytes;
    private final long rotationDelay;
    private final int maxArchives;
    private final RotationListener rotationListener;
    private final Executor rotationExecutor;
    private final OutputStream streamWrapper;
    private final AtomicBoolean closed = new AtomicBoolean( false );
    private final AtomicBoolean rotating = new AtomicBoolean( false );
    private final AtomicLong earliestRotationTimeRef = new AtomicLong( 0 );
    private final AtomicReference<OutputStream> outRef = new AtomicReference<>();
    private final List<WeakReference<OutputStream>> archivedStreams = new LinkedList<>();

    // Used only in case no new output file can be created during rotation
    private static final OutputStream nullStream = new OutputStream()
    {
        @Override
        public void write( int i ) throws IOException
        {
        }
    };

    /**
     * @param fileSystem The filesystem to use
     * @param outputFile The file that the latest {@link OutputStream} should output to
     * @param rotationThresholdBytes The size above which the file should be rotated
     * @param rotationDelay The minimum time (ms) after last rotation before the file may be rotated again
     * @param maxArchives The maximum number of archived output files to keep
     * @param rotationExecutor An {@link Executor} for performing the rotation
     * @throws IOException If the output file cannot be created
     */
    public RotatingFileOutputStreamSupplier( FileSystemAbstraction fileSystem, File outputFile,
            long rotationThresholdBytes, long rotationDelay, int maxArchives, Executor rotationExecutor )
            throws IOException
    {
        this( fileSystem, outputFile, rotationThresholdBytes, rotationDelay, maxArchives, rotationExecutor,
                new RotationListener() );
    }

    /**
     * @param fileSystem The filesystem to use
     * @param outputFile The file that the latest {@link OutputStream} should output to
     * @param rotationThresholdBytes The size above which the file should be rotated
     * @param rotationDelay The minimum time (ms) after last rotation before the file may be rotated again
     * @param maxArchives The maximum number of archived output files to keep
     * @param rotationExecutor An {@link Executor} for performing the rotation
     * @param rotationListener A {@link org.neo4j.logging.RotatingFileOutputStreamSupplier.RotationListener} that can
     * observe the rotation process and be notified of errors
     * @throws IOException If the output file cannot be created
     */
    public RotatingFileOutputStreamSupplier( FileSystemAbstraction fileSystem, File outputFile,
            long rotationThresholdBytes, long rotationDelay, int maxArchives, Executor rotationExecutor,
            RotationListener rotationListener ) throws IOException
    {
        this( DEFAULT_CURRENT_TIME_SUPPLIER, fileSystem, outputFile, rotationThresholdBytes, rotationDelay,
                maxArchives, rotationExecutor, rotationListener );
    }

    RotatingFileOutputStreamSupplier( LongSupplier currentTimeSupplier, FileSystemAbstraction fileSystem,
            File outputFile, long rotationThresholdBytes, long rotationDelay, int maxArchives,
            Executor rotationExecutor, RotationListener rotationListener ) throws IOException
    {
        this.currentTimeSupplier = currentTimeSupplier;
        this.fileSystem = fileSystem;
        this.outputFile = outputFile;
        this.rotationThresholdBytes = rotationThresholdBytes;
        this.rotationDelay = rotationDelay;
        this.maxArchives = maxArchives;
        this.rotationListener = rotationListener;
        this.rotationExecutor = rotationExecutor;
        this.outRef.set( openOutputFile() );
        // Wrap the actual reference to prevent race conditions during log rotation
        this.streamWrapper = new OutputStream()
        {
            @Override
            public void write( int i ) throws IOException
            {
                synchronized ( outRef )
                {
                    outRef.get().write( i );
                }
            }

            @Override
            public void write( byte[] bytes ) throws IOException
            {
                synchronized ( outRef )
                {
                    outRef.get().write( bytes );
                }
            }

            @Override
            public void write( byte[] bytes, int off, int len ) throws IOException
            {
                synchronized ( outRef )
                {
                    outRef.get().write( bytes, off, len );
                }
            }

            @Override
            public void flush() throws IOException
            {
                synchronized ( outRef )
                {
                    outRef.get().flush();
                }
            }
        };
    }

    /**
     * @return A stream outputting to the latest output file
     */
    @Override
    public OutputStream get()
    {
        if ( !closed.get() && !rotating.get() )
        {
            // In case output file doesn't exist, call rotate so that it gets created
            if ( rotationDelayExceeded() && rotationThresholdExceeded() ||
                    !fileSystem.fileExists( outputFile ) )
            {
                rotate();
            }
        }
        return this.streamWrapper;
    }

    @Override
    public void close() throws IOException
    {
        synchronized ( outRef )
        {
            closed.set( true );
            for ( WeakReference<OutputStream> archivedStream : archivedStreams )
            {
                OutputStream outputStream = archivedStream.get();
                if ( outputStream != null )
                {
                    try
                    {
                        outputStream.close();
                    }
                    catch ( Exception e )
                    {
                        // ignore
                    }
                }
            }
            outRef.get().close();
        }
    }

    private boolean rotationThresholdExceeded()
    {
        return fileSystem.fileExists( outputFile ) && rotationThresholdBytes > 0 &&
                fileSystem.getFileSize( outputFile ) >= rotationThresholdBytes;
    }

    private boolean rotationDelayExceeded()
    {
        return earliestRotationTimeRef.get() <= currentTimeSupplier.getAsLong();
    }

    private void rotate()
    {
        if ( rotating.getAndSet( true ) )
        {
            // Already rotating
            return;
        }

        Runnable runnable = () ->
        {
            synchronized ( outRef )
            {
                OutputStream oldStream = outRef.get();
                OutputStream newStream;

                try
                {
                    // Must close file prior to doing any operations on it or else it won't work on Windows
                    try
                    {
                        oldStream.flush();
                        oldStream.close();
                    }
                    catch ( Exception e )
                    {
                        rotationListener.rotationError( e, streamWrapper );
                        rotating.set( false );
                        return;
                    }

                    try
                    {
                        if ( fileSystem.fileExists( outputFile ) )
                        {
                            shiftArchivedOutputFiles();
                            fileSystem.renameFile( outputFile, archivedOutputFile( 1 ) );
                        }
                    }
                    catch ( Exception e )
                    {
                        rotationListener.rotationError( e, streamWrapper );
                        rotating.set( false );
                        return;
                    }

                }
                finally
                {
                    try
                    {
                        newStream = openOutputFile();
                    }
                    catch ( IOException e )
                    {
                        System.err.println( "Failed to open log file after log rotation: " + e.getMessage() );
                        newStream = nullStream;
                        rotationListener.rotationError( e, streamWrapper );
                        rotating.set( false );
                    }
                }

                if ( !closed.get() )
                {
                    outRef.set( newStream );
                    removeCollectedReferences( archivedStreams );
                    archivedStreams.add( new WeakReference<>( oldStream ) );
                }

                rotationListener.outputFileCreated( streamWrapper );

                if ( rotationDelay > 0 )
                {
                    earliestRotationTimeRef.set( currentTimeSupplier.getAsLong() + rotationDelay );
                }
                rotationListener.rotationCompleted( streamWrapper );
                rotating.set( false );
            }
        };

        try
        {
            rotationExecutor.execute( runnable );
        }
        catch ( Exception e )
        {
            rotationListener.rotationError( e, streamWrapper );
            rotating.set( false );
        }
    }

    private OutputStream openOutputFile() throws IOException
    {
        return createOrOpenAsOuputStream( fileSystem, outputFile, true );
    }

    private void shiftArchivedOutputFiles() throws IOException
    {
        for ( int i = lastArchivedOutputFileNumber(); i > 0; --i )
        {
            File archive = archivedOutputFile( i );
            if ( i >= maxArchives )
            {
                fileSystem.deleteFile( archive );
            }
            else
            {
                fileSystem.renameFile( archive, archivedOutputFile( i + 1 ) );
            }
        }
    }

    private int lastArchivedOutputFileNumber()
    {
        int i = 1;
        while ( fileSystem.fileExists( archivedOutputFile( i ) ) )
        {
            i++;
        }
        return i - 1;
    }

    private File archivedOutputFile( int archiveNumber )
    {
        return new File( String.format( "%s.%d", outputFile.getPath(), archiveNumber ) );
    }

    private static <T> void removeCollectedReferences( List<WeakReference<T>> referenceList )
    {
        for ( Iterator<WeakReference<T>> iterator = referenceList.iterator(); iterator.hasNext(); )
        {
            if ( iterator.next().get() == null )
            {
                iterator.remove();
            }
        }
    }
}
