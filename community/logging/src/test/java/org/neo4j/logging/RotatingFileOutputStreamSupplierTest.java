/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.adversaries.fs.AdversarialOutputStream;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.RotatingFileOutputStreamSupplier.RotationListener;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.logging.FormattedLog.OUTPUT_STREAM_CONVERTER;
import static org.neo4j.logging.RotatingFileOutputStreamSupplier.getAllArchives;

public class RotatingFileOutputStreamSupplierTest
{
    private static final long TEST_TIMEOUT_MILLIS = 10_000;
    private static final java.util.concurrent.Executor DIRECT_EXECUTOR = Runnable::run;
    private FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory( getClass(), fileSystem );
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    private File logFile;
    private File archiveLogFile1;
    private File archiveLogFile2;
    private File archiveLogFile3;
    private File archiveLogFile4;
    private File archiveLogFile5;
    private File archiveLogFile6;
    private File archiveLogFile7;
    private File archiveLogFile8;
    private File archiveLogFile9;

    @Before
    public void setup()
    {
        File logDir = testDirectory.directory();
        logFile = new File( logDir, "logfile.log" );
        archiveLogFile1 = new File( logDir, "logfile.log.1" );
        archiveLogFile2 = new File( logDir, "logfile.log.2" );
        archiveLogFile3 = new File( logDir, "logfile.log.3" );
        archiveLogFile4 = new File( logDir, "logfile.log.4" );
        archiveLogFile5 = new File( logDir, "logfile.log.5" );
        archiveLogFile6 = new File( logDir, "logfile.log.6" );
        archiveLogFile7 = new File( logDir, "logfile.log.7" );
        archiveLogFile8 = new File( logDir, "logfile.log.8" );
        archiveLogFile9 = new File( logDir, "logfile.log.9" );
    }

    @Test
    public void createsLogOnConstruction() throws Exception
    {
        new RotatingFileOutputStreamSupplier( fileSystem, logFile, 250000, 0, 10, DIRECT_EXECUTOR );
        assertThat( fileSystem.fileExists( logFile ), is( true ) );
    }

    @Test
    public void rotatesLogWhenSizeExceeded() throws Exception
    {
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0,
                10, DIRECT_EXECUTOR );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( false ) );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( false ) );

        write( supplier, "Short" );
        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( true ) );
    }

    @Test
    public void limitsNumberOfArchivedLogs() throws Exception
    {
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 2,
                DIRECT_EXECUTOR );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( false ) );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( false ) );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile3 ), is( false ) );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile3 ), is( false ) );
    }

    @Test( timeout = TEST_TIMEOUT_MILLIS )
    public void rotationShouldNotDeadlockOnListener() throws Exception
    {
        String logContent = "Output file created";
        final AtomicReference<Exception> listenerException = new AtomicReference<>( null );
        CountDownLatch latch = new CountDownLatch( 1 );
        RotationListener listener = new RotationListener()
        {
            @Override
            public void outputFileCreated( OutputStream out )
            {
                try
                {
                    Thread thread = new Thread( () ->
                    {
                        try
                        {
                            out.write( logContent.getBytes() );
                            out.flush();
                        }
                        catch ( IOException e )
                        {
                            listenerException.set( e );
                        }
                    } );
                    thread.start();
                    thread.join();
                }
                catch ( Exception e )
                {
                    listenerException.set( e );
                }
                super.outputFileCreated( out );
            }

            @Override
            public void rotationCompleted( OutputStream out )
            {
                latch.countDown();
            }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        DefaultFileSystemAbstraction defaultFileSystemAbstraction = new DefaultFileSystemAbstraction();
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( defaultFileSystemAbstraction,
                logFile, 0, 0, 10, executor, listener );

        OutputStream outputStream = supplier.get();
        LockingPrintWriter lockingPrintWriter = new LockingPrintWriter( outputStream );
        lockingPrintWriter.withLock( () ->
        {
            supplier.rotate();
            latch.await();
            return Void.TYPE;
        } );

        shutDownExecutor( executor );

        List<String> strings = Files.readAllLines( logFile.toPath() );
        String actual = String.join( "", strings );
        assertEquals( logContent, actual );
        assertNull( listenerException.get() );
    }

    private void shutDownExecutor( ExecutorService executor ) throws InterruptedException
    {
        executor.shutdown();
        boolean terminated = executor.awaitTermination( TEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS );
        if ( !terminated )
        {
            throw new IllegalStateException( "Rotation execution failed to complete within reasonable time." );
        }
    }

    @Test
    public void shouldNotRotateLogWhenSizeExceededButNotDelay() throws Exception
    {
        UpdatableLongSupplier clock = new UpdatableLongSupplier( System.currentTimeMillis() );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( clock, fileSystem, logFile,
                10, SECONDS.toMillis( 60 ), 10, DIRECT_EXECUTOR, new RotationListener() );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( false ) );

        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( false ) );

        write( supplier, "A string longer than 10 bytes" );

        clock.setValue( clock.getAsLong() + SECONDS.toMillis( 59 ) );
        write( supplier, "A string longer than 10 bytes" );

        clock.setValue( clock.getAsLong() + SECONDS.toMillis( 1 ) );
        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile3 ), is( false ) );
    }

    @Test
    public void shouldFindAllArchives() throws Exception
    {
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0, 2,
                DIRECT_EXECUTOR );

        write( supplier, "A string longer than 10 bytes" );
        write( supplier, "A string longer than 10 bytes" );

        assertThat( fileSystem.fileExists( logFile ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile1 ), is( true ) );
        assertThat( fileSystem.fileExists( archiveLogFile2 ), is( false ) );

        List<File> allArchives = getAllArchives( fileSystem, logFile );
        assertThat( allArchives.size(), is( 1 ) );
        assertThat( allArchives, hasItem( archiveLogFile1 ) );
    }

    @Test
    public void shouldNotifyListenerWhenNewLogIsCreated() throws Exception
    {
        final CountDownLatch allowRotationComplete = new CountDownLatch( 1 );
        final CountDownLatch rotationComplete = new CountDownLatch( 1 );
        String outputFileCreatedMessage = "Output file created";
        String rotationCompleteMessage = "Rotation complete";

        RotationListener rotationListener = spy( new RotationListener()
        {
            @Override
            public void outputFileCreated( OutputStream out )
            {
                try
                {
                    allowRotationComplete.await( 1L, TimeUnit.SECONDS );
                    out.write( outputFileCreatedMessage.getBytes() );
                }
                catch ( InterruptedException | IOException e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public void rotationCompleted( OutputStream out )
            {
                rotationComplete.countDown();
                try
                {
                    out.write( rotationCompleteMessage.getBytes() );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );

        ExecutorService rotationExecutor = Executors.newSingleThreadExecutor();
        try
        {
            RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0,
                    10, rotationExecutor, rotationListener );

            write( supplier, "A string longer than 10 bytes" );
            write( supplier, "A string longer than 10 bytes" );

            allowRotationComplete.countDown();
            rotationComplete.await( 1L, TimeUnit.SECONDS );

            verify( rotationListener ).outputFileCreated( any( OutputStream.class ) );
            verify( rotationListener ).rotationCompleted( any( OutputStream.class ) );
        }
        finally
        {
            shutDownExecutor( rotationExecutor );
        }
    }

    @Test
    public void shouldNotifyListenerOnRotationErrorDuringJobExecution() throws Exception
    {
        RotationListener rotationListener = mock( RotationListener.class );
        Executor executor = mock( Executor.class );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0,
                10, executor, rotationListener );
        OutputStream outputStream = supplier.get();

        RejectedExecutionException exception = new RejectedExecutionException( "text exception" );
        doThrow( exception ).when( executor ).execute( any( Runnable.class ) );

        write( supplier, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream ) );

        verify( rotationListener ).rotationError( exception, outputStream );
    }

    @Test
    public void shouldReattemptRotationAfterExceptionDuringJobExecution() throws Exception
    {
        RotationListener rotationListener = mock( RotationListener.class );
        Executor executor = mock( Executor.class );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 10, 0,
                10, executor, rotationListener );
        OutputStream outputStream = supplier.get();

        RejectedExecutionException exception = new RejectedExecutionException( "text exception" );
        doThrow( exception ).when( executor ).execute( any( Runnable.class ) );

        write( supplier, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream ) );
        assertThat( supplier.get(), is( outputStream ) );

        verify( rotationListener, times( 2 ) ).rotationError( exception, outputStream );
    }

    @Test
    public void shouldNotifyListenerOnRotationErrorDuringRotationIO() throws Exception
    {
        RotationListener rotationListener = mock( RotationListener.class );
        FileSystemAbstraction fs = spy( fileSystem );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fs, logFile, 10, 0, 10,
                DIRECT_EXECUTOR, rotationListener );
        OutputStream outputStream = supplier.get();

        IOException exception = new IOException( "text exception" );
        doThrow( exception ).when( fs ).renameFile( any( File.class ), any( File.class ) );

        write( supplier, "A string longer than 10 bytes" );
        assertThat( supplier.get(), is( outputStream ) );

        verify( rotationListener ).rotationError( eq( exception ), any( OutputStream.class ) );
    }

    @Test
    public void shouldNotUpdateOutputStreamWhenClosedDuringRotation() throws Exception
    {
        final CountDownLatch allowRotationComplete = new CountDownLatch( 1 );

        RotationListener rotationListener = spy( new RotationListener()
        {
            @Override
            public void outputFileCreated( OutputStream out )
            {
                try
                {
                    allowRotationComplete.await();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );

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

        ExecutorService rotationExecutor = Executors.newSingleThreadExecutor();
        try
        {
            RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fs, logFile, 10, 0,
                    10, rotationExecutor, rotationListener );
            OutputStream outputStream = supplier.get();

            write( supplier, "A string longer than 10 bytes" );
            assertThat( supplier.get(), is( outputStream ) );

            allowRotationComplete.countDown();
            supplier.close();
        }
        finally
        {
            shutDownExecutor( rotationExecutor );
        }

        assertStreamClosed( mockStreams.get( 0 ) );
    }

    @Test
    public void shouldCloseAllOutputStreams() throws Exception
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

        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fs, logFile, 10, 0,
                10, DIRECT_EXECUTOR );

        write( supplier, "A string longer than 10 bytes" );

        supplier.close();

        assertStreamClosed( mockStreams.get( 0 ) );
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

        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( fs, logFile, 10, 0, 10,
                DIRECT_EXECUTOR );

        write( supplier, "A string longer than 10 bytes" );
        write( supplier, "A string longer than 10 bytes" );

        IOException exception = new IOException( "test exception" );
        OutputStream mockStream = mockStreams.get( 1 );
        doThrow( exception ).when( mockStream ).close();

        try
        {
            supplier.close();
            fail();
        }
        catch ( IOException e )
        {
            assertThat( e, sameInstance( exception ) );
        }
        verify( mockStream ).close();
    }

    @Test
    public void shouldSurviveFilesystemErrors() throws Exception
    {
        final RandomAdversary adversary = new RandomAdversary( 0.1, 0.1, 0 );
        adversary.setProbabilityFactor( 0 );

        AdversarialFileSystemAbstraction adversarialFileSystem = new SensibleAdversarialFileSystemAbstraction(
                adversary, fileSystem );
        RotatingFileOutputStreamSupplier supplier = new RotatingFileOutputStreamSupplier( adversarialFileSystem,
                logFile, 1000, 0, 9, DIRECT_EXECUTOR );

        adversary.setProbabilityFactor( 1 );
        writeLines( supplier, 10000 );

        // run cleanly for a while, to allow it to fill any gaps left in log archive numbers
        adversary.setProbabilityFactor( 0 );
        writeLines( supplier, 10000 );

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

    private void write( RotatingFileOutputStreamSupplier supplier, String line )
    {
        PrintWriter writer = new PrintWriter( supplier.get() );
        writer.println( line );
        writer.flush();
    }

    private void writeLines( Supplier<OutputStream> outputStreamSupplier, int count )
    {
        Supplier<PrintWriter> printWriterSupplier = Suppliers.adapted( outputStreamSupplier, OUTPUT_STREAM_CONVERTER );
        for ( ; count >= 0; --count )
        {
            printWriterSupplier.get().println(
                    "We are what we repeatedly do. Excellence, then, is not an act, but a habit." );
            Thread.yield();
        }
    }

    private void assertStreamClosed( OutputStream stream ) throws IOException
    {
        try
        {
            stream.write( 0 );
            fail( "Expected ClosedChannelException" );
        }
        catch ( ClosedChannelException e )
        {
            // expected
        }
    }

    private class LockingPrintWriter extends PrintWriter
    {
        LockingPrintWriter( OutputStream out )
        {
            super( out );
            lock = new Object();

        }

        void withLock( Callable callable ) throws Exception
        {
            synchronized ( lock )
            {
                callable.call();
            }
        }
    }

    private static class UpdatableLongSupplier implements LongSupplier
    {
        private final AtomicLong longValue;

        UpdatableLongSupplier( long value )
        {
            this.longValue = new AtomicLong( value );
        }

        long setValue( long value )
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

        @Override
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
