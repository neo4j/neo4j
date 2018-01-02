/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.function.LongSupplier;
import org.neo4j.function.Supplier;
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
        public void outputFileCreated( OutputStream newStream, OutputStream oldStream )
        {
        }

        public void rotationCompleted( OutputStream newStream, OutputStream oldStream )
        {
        }

        public void rotationError( @SuppressWarnings("unused") Exception e, @SuppressWarnings("unused") OutputStream out )
        {
        }
    }

    private static final LongSupplier DEFAULT_CURRENT_TIME_SUPPLIER = new LongSupplier()
    {
        @Override
        public long getAsLong()
        {
            return System.currentTimeMillis();
        }
    };

    private final LongSupplier currentTimeSupplier;
    private final FileSystemAbstraction fileSystem;
    private final File outputFile;
    private final long rotationThresholdBytes;
    private final long rotationDelay;
    private final int maxArchives;
    private final RotationListener rotationListener;
    private final Executor rotationExecutor;
    private final AtomicBoolean closed = new AtomicBoolean( false );
    private final AtomicBoolean rotating = new AtomicBoolean( false );
    private final AtomicLong earliestRotationTimeRef = new AtomicLong( 0 );
    private final AtomicReference<OutputStream> outRef = new AtomicReference<>();
    private final List<WeakReference<OutputStream>> archivedStreams = new LinkedList<>();

    /**
     * @param fileSystem             The filesystem to use
     * @param outputFile             The file that the latest {@link OutputStream} should output to
     * @param rotationThresholdBytes The size above which the file should be rotated
     * @param rotationDelay          The minimum time (ms) after last rotation before the file may be rotated again
     * @param maxArchives            The maximum number of archived output files to keep
     * @param rotationExecutor       An {@link Executor} for performing the rotation
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
     * @param fileSystem             The filesystem to use
     * @param outputFile             The file that the latest {@link OutputStream} should output to
     * @param rotationThresholdBytes The size above which the file should be rotated
     * @param rotationDelay          The minimum time (ms) after last rotation before the file may be rotated again
     * @param maxArchives            The maximum number of archived output files to keep
     * @param rotationExecutor       An {@link Executor} for performing the rotation
     * @param rotationListener       A {@link org.neo4j.logging.RotatingFileOutputStreamSupplier.RotationListener} that can observe the rotation process and be notified of errors
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
    }

    /**
     * @return An stream outputting to the latest output file
     */
    @Override
    public OutputStream get()
    {
        if ( !closed.get() && !rotating.get() )
        {
            if ( rotationDelayExceeded() && rotationThresholdExceeded() )
            {
                rotate();
            }
        }
        return outRef.get();
    }

    @Override
    public void close() throws IOException
    {
        synchronized (outRef)
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
        return rotationThresholdBytes == 0 || !fileSystem.fileExists( outputFile ) || fileSystem.getFileSize( outputFile ) >= rotationThresholdBytes;
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

        Runnable runnable = new Runnable()
        {
            @Override
            public void run()
            {
                OutputStream newStream;
                try
                {
                    if ( fileSystem.fileExists( outputFile ) )
                    {
                        shiftArchivedOutputFiles();
                        fileSystem.renameFile( outputFile, archivedOutputFile( 1 ) );
                    }
                    newStream = openOutputFile();
                }
                catch ( Exception e )
                {
                    rotationListener.rotationError( e, outRef.get() );
                    rotating.set( false );
                    return;
                }
                OutputStream oldStream = outRef.get();
                rotationListener.outputFileCreated( newStream, oldStream );
                synchronized ( outRef )
                {
                    if ( !closed.get() )
                    {
                        outRef.set( newStream );
                        removeCollectedReferences( archivedStreams );
                        archivedStreams.add( new WeakReference<>( oldStream ) );
                    }
                }
                if ( rotationDelay > 0 )
                {
                    earliestRotationTimeRef.set( currentTimeSupplier.getAsLong() + rotationDelay );
                }
                rotationListener.rotationCompleted( newStream, oldStream );
                rotating.set( false );
            }
        };

        try
        {
            rotationExecutor.execute( runnable );
        }
        catch ( Exception e )
        {
            rotationListener.rotationError( e, outRef.get() );
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
            } else
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
