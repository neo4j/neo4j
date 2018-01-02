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

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.adversaries.fs.AdversarialOutputStream;
import org.neo4j.function.LongSupplier;
import org.neo4j.function.Supplier;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.RotatingFileOutputStreamSupplier.RotationListener;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.logging.FormattedLog.OUTPUT_STREAM_CONVERTER;

public class RotatingFileOutputStreamSupplierTest
{
    private static final java.util.concurrent.Executor DIRECT_EXECUTOR = new Executor()
    {
        @Override
        public void execute( Runnable task )
        {
            task.run();
        }
    };

    private FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    private File logFile = new File( "/tmp/logfile.log" );
    private File archiveLogFile1 = new File( "/tmp/logfile.log.1" );
    private File archiveLogFile2 = new File( "/tmp/logfile.log.2" );
    private File archiveLogFile3 = new File( "/tmp/logfile.log.3" );
    private File archiveLogFile4 = new File( "/tmp/logfile.log.4" );
    private File archiveLogFile5 = new File( "/tmp/logfile.log.5" );
    private File archiveLogFile6 = new File( "/tmp/logfile.log.6" );
    private File archiveLogFile7 = new File( "/tmp/logfile.log.7" );
    private File archiveLogFile8 = new File( "/tmp/logfile.log.8" );
    private File archiveLogFile9 = new File( "/tmp/logfile.log.9" );

    @Test
    public void createsLogOnConstruction() throws Exception
    {
        new RotatingFileOutputStreamSupplier( fileSystem, logFile, 250000, 0, 10, DIRECT_EXECUTOR );
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
    }

    @Test
    public void rotatesLogWhenSizeExceeded() throws Exception
    {
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 10, DIRECT_EXECUTOR );
        OutputStream outputStream = supplier.get();
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( false ) );

        write( outputStream, "A string longer than 10 bytes" );
        OutputStream outputStream2 = supplier.get();
        assertThat( outputStream2, not( outputStream ) );
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( false ) );

        write( outputStream2, "Short" );
        assertThat( supplier.get(), is( outputStream2 ) );

        write( outputStream2, "A string longer than 10 bytes" );
        OutputStream outputStream3 = supplier.get();
        assertThat( outputStream3, not( outputStream2 ) );
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( true ) );
    }

    @Test
    public void limitsNumberOfArchivedLogs() throws Exception
    {
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 2, DIRECT_EXECUTOR );
        OutputStream outputStream = supplier.get();
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( false ) );

        write( outputStream, "A string longer than 10 bytes" );
        OutputStream outputStream2 = supplier.get();
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( false ) );

        write( outputStream2, "A string longer than 10 bytes" );
        OutputStream outputStream3 = supplier.get();
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile3 ), is( false ) );

        write( outputStream3, "A string longer than 10 bytes" );
        supplier.get();
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile3 ), is( false ) );
    }

    @Test
    public void shouldReturnSameStreamWhilstRotationOccurs() throws Exception
    {
        ManualExecutor executor = new ManualExecutor();
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 10, executor );
        OutputStream outputStream = supplier.get();

        write( outputStream, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream ) );
        assertThat( executor.isScheduled(), is( true ) );

        write( outputStream, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream ) );

        executor.runTask();

        assertThat( supplier.get(), not( outputStream ) );
    }

    @Test
    public void shouldNotRotatesLogWhenSizeExceededByNotDelay() throws Exception
    {
        UpdatableLongSupplier clock = new UpdatableLongSupplier( System.currentTimeMillis() );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( clock, fileSystem, logFile,
                10, SECONDS.toMillis( 60 ), 10, DIRECT_EXECUTOR, new RotationListener() );
        OutputStream outputStream = supplier.get();
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( false ) );

        write( outputStream, "A string longer than 10 bytes" );
        OutputStream outputStream2 = supplier.get();
        assertThat( outputStream2, not( outputStream ) );
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( false ) );

        write( outputStream2, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream2 ) );

        clock.setValue( clock.getAsLong() + SECONDS.toMillis( 59 ) );
        write( outputStream2, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream2 ) );

        clock.setValue( clock.getAsLong() + SECONDS.toMillis( 1 ) );
        write( outputStream2, "A string longer than 10 bytes" );
        OutputStream outputStream3 = supplier.get();
        assertThat( outputStream3, not( outputStream2 ) );
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile3 ), is( false ) );
    }

    @Test
    public void shouldNotifyListenerWhenNewLogIsCreated() throws Exception
    {
        final CountDownLatch allowRotationComplete = new CountDownLatch( 1 );
        final CountDownLatch rotationComplete = new CountDownLatch( 1 );

        RotationListener rotationListener = spy( new RotationListener()
        {
            @Override
            public void outputFileCreated( OutputStream newStream, OutputStream oldStream )
            {
                try
                {
                    allowRotationComplete.await();
                } catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public void rotationCompleted( OutputStream newStream, OutputStream oldStream )
            {
                rotationComplete.countDown();
            }
        } );

        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 10, Executors.newSingleThreadExecutor(), rotationListener );
        OutputStream outputStream = supplier.get();

        write( outputStream, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream ) );

        allowRotationComplete.countDown();
        rotationComplete.await();

        OutputStream outputStream2 = supplier.get();
        assertThat( outputStream2, not( outputStream ) );

        verify( rotationListener ).outputFileCreated( outputStream2, outputStream );
        verify( rotationListener ).rotationCompleted( outputStream2, outputStream );
    }

    @Test
    public void shouldNotifyListenerOnRotationErrorDuringJobExecution() throws Exception
    {
        RotationListener rotationListener = mock( RotationListener.class );
        Executor executor = mock( Executor.class );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 10, executor, rotationListener );
        OutputStream outputStream = supplier.get();

        RejectedExecutionException exception = new RejectedExecutionException( "text exception" );
        doThrow( exception ).when( executor ).execute( any( Runnable.class ) );

        write( outputStream, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream ) );

        verify( rotationListener ).rotationError( exception, outputStream );
    }

    @Test
    public void shouldReattemptRotationAfterExceptionDuringJobExecution() throws Exception
    {
        RotationListener rotationListener = mock( RotationListener.class );
        Executor executor = mock( Executor.class );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 10, executor, rotationListener );
        OutputStream outputStream = supplier.get();

        RejectedExecutionException exception = new RejectedExecutionException( "text exception" );
        doThrow( exception ).when( executor ).execute( any( Runnable.class ) );

        write( outputStream, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream ) );
        assertThat( supplier.get(), is( outputStream ) );

        verify( rotationListener, times( 2 ) ).rotationError( exception, outputStream );
    }

    @Test
    public void shouldNotifyListenerOnRotationErrorDuringRotationIO() throws Exception
    {
        RotationListener rotationListener = mock( RotationListener.class );
        FileSystemAbstraction fs = spy( fileSystem );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fs, logFile, 10, 0, 10, DIRECT_EXECUTOR, rotationListener );
        OutputStream outputStream = supplier.get();

        IOException exception = new IOException( "text exception" );
        doThrow( exception ).when( fs ).renameFile( any( File.class ), any( File.class ) );

        write( outputStream, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream ) );

        verify( rotationListener ).rotationError( exception, outputStream );
    }

    @Test
    public void shouldNotUpdateOutputStreamWhenClosedDuringRotation() throws Exception
    {
        final CountDownLatch allowRotationComplete = new CountDownLatch( 1 );

        RotationListener rotationListener = spy( new RotationListener()
        {
            @Override
            public void outputFileCreated( OutputStream newStream, OutputStream oldStream )
            {
                try
                {
                    allowRotationComplete.await();
                } catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );

        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 10, Executors.newSingleThreadExecutor(), rotationListener );
        OutputStream outputStream = supplier.get();

        write( outputStream, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream ) );

        allowRotationComplete.countDown();
        supplier.close();

        assertStreamClosed( supplier.get() );
    }

    @Test
    public void shouldCloseAllOutputStreams() throws Exception
    {
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 10, DIRECT_EXECUTOR );
        OutputStream outputStream = supplier.get();

        write( outputStream, "A string longer than 10 bytes" );
        OutputStream outputStream2 = supplier.get();
        assertThat( outputStream2, not( outputStream ) );

        supplier.close();

        assertStreamClosed( outputStream );
        assertStreamClosed( outputStream2 );
    }

    @Test
    public void shouldCloseAllStreamsDespiteError() throws Exception
    {
        final List<OutputStream> mockStreams = new ArrayList<>();
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( fileSystem )
        {
            @Override
            public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
            {
                final OutputStream stream = spy( super.openAsOutputStream( fileName, append ) );
                mockStreams.add( stream );
                return stream;
            }
        };

        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fs, logFile, 10, 0, 10, DIRECT_EXECUTOR );
        OutputStream outputStream = supplier.get();
        assertThat( outputStream, sameInstance( mockStreams.get( 0 ) ) );

        write( outputStream, "A string longer than 10 bytes" );
        OutputStream outputStream2 = supplier.get();
        assertThat( outputStream2, sameInstance( mockStreams.get( 1 ) ) );

        IOException exception1 = new IOException( "test exception" );
        doThrow( exception1 ).when( outputStream ).close();

        IOException exception2 = new IOException( "test exception" );
        doThrow( exception2 ).when( outputStream2 ).close();

        try
        {
            supplier.close();
        }
        catch ( IOException e )
        {
            assertThat( e, sameInstance( exception2 ) );
        }
        verify( outputStream ).close();
    }

    @Test
    public void shouldSurviveFilesystemErrors() throws Exception
    {
        final RandomAdversary adversary = new RandomAdversary( 0.1, 0.1, 0 );
        adversary.setProbabilityFactor( 0 );

        AdversarialFileSystemAbstraction adversarialFileSystem = new SensibleAdversarialFileSystemAbstraction( adversary, fileSystem );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( adversarialFileSystem, logFile, 1000, 0, 9, DIRECT_EXECUTOR );

        adversary.setProbabilityFactor( 1 );
        writeLines( supplier, 10000 );

        // run cleanly for a while, to allow it to fill any gaps left in log archive numbers
        adversary.setProbabilityFactor( 0 );
        writeLines( supplier, 1000 );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile3 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile4 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile5 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile6 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile7 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile8 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile9 ), is( true ) );
    }

    private void write( OutputStream outputStream, String line )
    {
        PrintWriter writer = new PrintWriter( outputStream );
        writer.println( line );
        writer.flush();
    }

    private void writeLines( Supplier<OutputStream> outputStreamSupplier, int count ) throws InterruptedException
    {
        Supplier<PrintWriter> printWriterSupplier = Suppliers.adapted( outputStreamSupplier, OUTPUT_STREAM_CONVERTER );
        for (; count >= 0; --count )
        {
            printWriterSupplier.get().println( "We are what we repeatedly do. Excellence, then, is not an act, but a habit." );
            Thread.yield();
        }
    }

    private void assertStreamClosed( OutputStream stream ) throws IOException
    {
        try
        {
            stream.write( 0 );
            fail( "Expected ClosedChannelException" );
        } catch ( ClosedChannelException e )
        {
            // expected
        }
    }

    private class ManualExecutor implements Executor
    {
        private Runnable task;

        @Override
        public void execute( Runnable task )
        {
            if ( isScheduled() )
            {
                throw new IllegalStateException( "task already scheduled with Executor" );
            }
            this.task = task;
        }

        public boolean isScheduled()
        {
            return task != null;
        }

        public void runTask()
        {
            if ( !isScheduled() )
            {
                throw new IllegalStateException( "task not scheduled with Executor" );
            }
            task.run();
            task = null;
        }
    }

    private static class UpdatableLongSupplier implements LongSupplier
    {
        private final AtomicLong longValue;

        UpdatableLongSupplier( long value )
        {
            this.longValue = new AtomicLong( value );
        }

        public long setValue( long value )
        {
            return longValue.getAndSet( value );
        }

        @Override
        public long getAsLong()
        {
            return longValue.get();
        }
    }

    private static class SensibleAdversarialFileSystemAbstraction extends AdversarialFileSystemAbstraction
    {
        private final Adversary adversary;
        private final FileSystemAbstraction delegate;

        SensibleAdversarialFileSystemAbstraction( Adversary adversary, FileSystemAbstraction delegate )
        {
            super( adversary, delegate );
            this.adversary = adversary;
            this.delegate = delegate;
        }

        @Override
        public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
        {
            // Default adversarial might throw a java.lang.SecurityException here, which is an exception
            // that should not be survived
            adversary.injectFailure( FileNotFoundException.class );
            final OutputStream outputStream = delegate.openAsOutputStream( fileName, append );
            return new AdversarialOutputStream( outputStream, adversary )
            {
                @Override
                public void write( byte[] b ) throws IOException
                {
                    // Default adversarial might throw a NullPointerException.class or IndexOutOfBoundsException here,
                    // which are exceptions that should not be survived
                    adversary.injectFailure( IOException.class );
                    outputStream.write( b );
                }

                @Override
                public void write( byte[] b, int off, int len ) throws IOException
                {
                    // Default adversarial might throw a NullPointerException.class or IndexOutOfBoundsException here,
                    // which are exceptions that should not be survived
                    adversary.injectFailure( IOException.class );
                    outputStream.write( b, off, len );
                }
            };
        }

        public boolean fileExists( File fileName )
        {
            // Default adversarial might throw a java.lang.SecurityException here, which is an exception
            // that should not be survived
            return delegate.fileExists( fileName );
        }

        @Override
        public long getFileSize( File fileName )
        {
            // Default adversarial might throw a java.lang.SecurityException here, which is an exception
            // that should not be survived
            return delegate.getFileSize( fileName );
        }
    }
}
