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
package org.neo4j.com;

import org.junit.Rule;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.neo4j.function.Function;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.AbstractStoreChannel;
import org.neo4j.io.fs.FileLock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.TargetDirectory;

public class ServerUtilTest
{
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    /*
    @Test
    public void shouldIgnoreLogicalLogsWhenCopyingFilesForBackup() throws IOException
    {
        // given
        final FileSystemAbstraction fs = new StubFileSystemAbstraction();

        XaDataSource dataSource = mock( XaDataSource.class );

        FileResourceIterator storeFiles = new FileResourceIterator( fs, testDirectory, "neostore.nodestore.db" );
        FileResourceIterator logicalLogs = new FileResourceIterator( fs, testDirectory,
        PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "0" );

        when( dataSource.listStoreFiles() ).thenReturn( storeFiles );
        when( dataSource.listLogicalLogs() ).thenReturn( logicalLogs );
        when( dataSource.getBranchId() ).thenReturn( "branch".getBytes() );
        when( dataSource.getName() ).thenReturn( "branch" );

        XaContainer xaContainer = mock( XaContainer.class );
        when( dataSource.getXaContainer() ).thenReturn( xaContainer );

        XaLogicalLog xaLogicalLog = mock( XaLogicalLog.class );

        when( xaContainer.getLogicalLog() ).thenReturn( xaLogicalLog );

        XaResourceManager xaResourceManager = mock( XaResourceManager.class );
        when( xaContainer.getResourceManager() ).thenReturn( xaResourceManager );

        XaDataSourceManager dsManager = new XaDataSourceManager( StringLogger.DEV_NULL );
        dsManager.registerDataSource( dataSource );

        KernelPanicEventGenerator kernelPanicEventGenerator = mock( KernelPanicEventGenerator.class );
        StoreWriter storeWriter = mock( StoreWriter.class );

        // when
        ServerUtil.rotateLogsAndStreamStoreFiles( testDirectory.absolutePath(), dsManager, kernelPanicEventGenerator,
                StringLogger.DEV_NULL, false, storeWriter, fs, StoreCopyMonitor.NONE );

        // then
        verify( storeWriter ).write( eq( "neostore.nodestore.db" ), any( ReadableByteChannel.class ),
                any( ByteBuffer.class ), any( Boolean.class ) );
        verify( storeWriter, never() ).write( eq( PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "0" ), any( ReadableByteChannel.class ),
                any( ByteBuffer.class ), any( Boolean.class ) );

    }

    @Test
    public void shouldCopyLogicalLogFile() throws IOException
    {
        // given
        final FileSystemAbstraction fs = new StubFileSystemAbstraction();

        XaDataSource dataSource = mock( XaDataSource.class );

        FileResourceIterator storeFiles = new FileResourceIterator( fs, testDirectory );
        FileResourceIterator logicalLogs = new FileResourceIterator( fs, testDirectory, PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "0" );

        when( dataSource.listStoreFiles() ).thenReturn( storeFiles );
        when( dataSource.listLogicalLogs() ).thenReturn( logicalLogs );
        when( dataSource.getBranchId() ).thenReturn( "branch".getBytes() );
        when( dataSource.getName() ).thenReturn( "branch" );

        XaContainer xaContainer = mock( XaContainer.class );
        when( dataSource.getXaContainer() ).thenReturn( xaContainer );

        XaLogicalLog xaLogicalLog = mock( XaLogicalLog.class );

        when( xaContainer.getLogicalLog() ).thenReturn( xaLogicalLog );

        XaResourceManager xaResourceManager = mock( XaResourceManager.class );
        when( xaContainer.getResourceManager() ).thenReturn( xaResourceManager );

        XaDataSourceManager dsManager = new XaDataSourceManager( StringLogger.DEV_NULL );
        dsManager.registerDataSource( dataSource );

        KernelPanicEventGenerator kernelPanicEventGenerator = mock( KernelPanicEventGenerator.class );
        StoreWriter storeWriter = mock( StoreWriter.class );

        // when
        ServerUtil.rotateLogsAndStreamStoreFiles( testDirectory.absolutePath(), dsManager, kernelPanicEventGenerator,
                StringLogger.DEV_NULL, true, storeWriter, fs, StoreCopyMonitor.NONE );

        // then
        verify( storeWriter ).write( eq( PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "0" ), any( ReadableByteChannel.class ),
                any( ByteBuffer.class ), any( Boolean.class ) );
    }

    @Test
    public void shouldNotThrowFileNotFoundExceptionWhenTryingToCopyAMissingLogicalLogFile() throws IOException
    {
        // given
        final FileSystemAbstraction fs = new StubFileSystemAbstraction();

        XaDataSource dataSource = mock( XaDataSource.class );

        FileResourceIterator storeFiles = new FileResourceIterator( fs, testDirectory, "neostore.nodestore.db" );

        FileResourceIterator logicalLogs = new FileResourceIterator( fs, testDirectory,
        PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "0" );
        logicalLogs.deleteBeforeCopy( PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "0" );

        when( dataSource.listStoreFiles() ).thenReturn( storeFiles );
        when( dataSource.listLogicalLogs() ).thenReturn( logicalLogs );

        when( dataSource.getBranchId() ).thenReturn( "branch".getBytes() );
        when( dataSource.getName() ).thenReturn( "branch" );

        XaContainer xaContainer = mock( XaContainer.class );
        when( dataSource.getXaContainer() ).thenReturn( xaContainer );

        XaResourceManager xaResourceManager = mock( XaResourceManager.class );
        when( xaContainer.getResourceManager() ).thenReturn( xaResourceManager );

        XaDataSourceManager dsManager = new XaDataSourceManager( StringLogger.DEV_NULL );
        dsManager.registerDataSource( dataSource );

        KernelPanicEventGenerator kernelPanicEventGenerator = mock( KernelPanicEventGenerator.class );
        StoreWriter storeWriter = mock( StoreWriter.class );

        // when
        ServerUtil.rotateLogsAndStreamStoreFiles( testDirectory.absolutePath(), dsManager, kernelPanicEventGenerator,
                StringLogger.DEV_NULL, true, storeWriter, fs, StoreCopyMonitor.NONE );

        // then
        verify( storeWriter ).write( eq( "neostore.nodestore.db" ), any( ReadableByteChannel.class ),
                any( ByteBuffer.class ), any( Boolean.class ) );
    }

    @Test
    public void shouldThrowFileNotFoundExceptionWhenTryingToCopyAStoreFileWhichDoesNotExist() throws IOException
    {
        // given
        final FileSystemAbstraction fs = new StubFileSystemAbstraction();

        XaDataSource dataSource = mock( XaDataSource.class );

        FileResourceIterator storeFiles = new FileResourceIterator( fs, testDirectory, "neostore.nodestore.db" );
        storeFiles.deleteBeforeCopy( "neostore.nodestore.db" );

        FileResourceIterator logicalLogs = new FileResourceIterator( fs, testDirectory );

        when( dataSource.listStoreFiles() ).thenReturn( storeFiles );
        when( dataSource.listLogicalLogs() ).thenReturn( logicalLogs );


        when( dataSource.getBranchId() ).thenReturn( "branch".getBytes() );
        when( dataSource.getName() ).thenReturn( "branch" );

        XaContainer xaContainer = mock( XaContainer.class );
        when( dataSource.getXaContainer() ).thenReturn( xaContainer );

        XaResourceManager xaResourceManager = mock( XaResourceManager.class );
        when( xaContainer.getResourceManager() ).thenReturn( xaResourceManager );

        XaDataSourceManager dsManager = new XaDataSourceManager( StringLogger.DEV_NULL );
        dsManager.registerDataSource( dataSource );

        KernelPanicEventGenerator kernelPanicEventGenerator = mock( KernelPanicEventGenerator.class );
        StoreWriter storeWriter = mock( StoreWriter.class );

        // when
        try
        {
            ServerUtil.rotateLogsAndStreamStoreFiles( testDirectory.absolutePath(), dsManager,
                    kernelPanicEventGenerator,
                    StringLogger.DEV_NULL, true, storeWriter, fs, StoreCopyMonitor.NONE );
            fail( "should have thrown exception" );
        }
        catch ( ServerFailureException e )
        {
            // then
            assertEquals( java.io.FileNotFoundException.class, e.getCause().getClass() );
        }
    }
*/
    private static class FileResourceIterator implements ResourceIterator<File>
    {
        private final FileSystemAbstraction fs;
        private final TargetDirectory.TestDirectory testDirectory;
        private final Queue<String> files;
        private String nextFilePath;
        private final List<String> filesToDelete = new ArrayList<>();

        public FileResourceIterator( FileSystemAbstraction fs, TargetDirectory.TestDirectory testDirectory,
                                     String... files )
        {
            this.fs = fs;
            this.testDirectory = testDirectory;
            this.files = new ArrayBlockingQueue<>( files.length == 0 ? 1 : files.length, true, Arrays.asList( files ) );
        }

        @Override
        public void close()
        {

        }

        @Override
        public boolean hasNext()
        {
            nextFilePath = files.poll();
            return nextFilePath != null;
        }

        @Override
        public File next()
        {
            File file = new File( String.format( "%s/%s", testDirectory.directory(), nextFilePath ) );
            try
            {
                fs.create( file );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }

            if ( filesToDelete.contains( nextFilePath ) )
            {
                fs.deleteFile( file );
            }

            return file;
        }

        @Override
        public void remove()
        {

        }

        public void deleteBeforeCopy( String filePath )
        {
            filesToDelete.add( filePath );
        }
    }

    private class StubFileSystemAbstraction implements FileSystemAbstraction
    {
        private final List<File> files = new ArrayList<>();

        @Override
        public StoreChannel open( File fileName, String mode ) throws IOException
        {
            if ( files.contains( fileName ) )
            {
                return new AbstractStoreChannel() {
                    @Override
                    public void close() throws IOException
                    {
                    }
                };
            }
            throw new FileNotFoundException( fileName.getPath() );
        }

        @Override
        public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
        {
            return null;
        }

        @Override
        public InputStream openAsInputStream( File fileName ) throws IOException
        {
            return null;
        }

        @Override
        public Reader openAsReader( File fileName, String encoding ) throws IOException
        {
            return null;
        }

        @Override
        public Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException
        {
            return null;
        }

        @Override
        public FileLock tryLock( File fileName, StoreChannel channel ) throws IOException
        {
            return null;
        }

        @Override
        public StoreChannel create( File fileName ) throws IOException
        {
            files.add( fileName );
            return null;
        }

        @Override
        public boolean fileExists( File fileName )
        {
            return files.contains( fileName );
        }

        @Override
        public boolean mkdir( File fileName )
        {
            return false;
        }

        @Override
        public void mkdirs( File fileName ) throws IOException
        {

        }

        @Override
        public long getFileSize( File fileName )
        {
            return 0;
        }

        @Override
        public boolean deleteFile( File fileName )
        {
            files.remove( fileName );
            return false;
        }

        @Override
        public void deleteRecursively( File directory ) throws IOException
        {

        }

        @Override
        public boolean renameFile( File from, File to ) throws IOException
        {
            return false;
        }

        @Override
        public File[] listFiles( File directory )
        {
            return new File[0];
        }

        @Override
        public File[] listFiles( File directory, FilenameFilter filter )
        {
            return new File[0];
        }

        @Override
        public boolean isDirectory( File file )
        {
            return false;
        }

        @Override
        public void moveToDirectory( File file, File toDirectory ) throws IOException
        {

        }

        @Override
        public void copyFile( File from, File to ) throws IOException
        {

        }

        @Override
        public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
        {

        }

        @Override
        public <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem(
                Class<K> clazz, Function<Class<K>,K> creator )
        {
            return null;
        }
    }
}
