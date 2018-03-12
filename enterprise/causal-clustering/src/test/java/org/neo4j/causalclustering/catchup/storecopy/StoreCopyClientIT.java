/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.compress.utils.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.causalclustering.StrippedCatchupServer;
import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.collection.primitive.base.Empty;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StoreCopyClientIT
{
    FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
    private final LogProvider logProvider = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );
    private final TerminationCondition defaultTerminationCondition = TerminationCondition.CONTINUE_INDEFINITELY;

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemAbstraction );

    private FakeCatchupServer testCatchupServer;

    private StoreCopyClient subject;
    private FakeFile fileA = new FakeFile( "fileA", "This is file a content" );
    private FakeFile fileB = new FakeFile( "another-file-b", "Totally different content 123" );

    private FakeFile indexFileA = new FakeFile( "lucene", "Lucene 123" );

    private static void writeContents( FileSystemAbstraction fileSystemAbstraction, File file, String contents )
    {
        byte[] bytes = contents.getBytes();
        try ( StoreChannel storeChannel = fileSystemAbstraction.create( file ) )
        {
            storeChannel.write( ByteBuffer.wrap( bytes ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Before
    public void setup()
    {
        testCatchupServer = new FakeCatchupServer( this, logProvider );
        testCatchupServer.addFile( fileA );
        testCatchupServer.addFile( fileB );
        testCatchupServer.addIndexFile( indexFileA );
        writeContents( fileSystemAbstraction, relative( fileA.getFilename() ), fileA.getContent() );
        writeContents( fileSystemAbstraction, relative( fileB.getFilename() ), fileB.getContent() );
        writeContents( fileSystemAbstraction, relative( indexFileA.getFilename() ), indexFileA.getContent() );

        CatchUpClient catchUpClient =
                new CatchUpClient( logProvider, Clock.systemDefaultZone(), 2000, new Monitors(), VoidPipelineWrapperFactory.VOID_WRAPPER );
        catchUpClient.start();
        subject = new StoreCopyClient( catchUpClient, logProvider );

        testCatchupServer.before();
        testCatchupServer.start();
    }

    @After
    public void shutdown()
    {
        testCatchupServer.stop();
    }

    @Test
    public void canPerformCatchup() throws StoreCopyFailedException
    {
        // given local client has a store
        InMemoryFileSystemStream storeFileStream = new InMemoryFileSystemStream();

        // when catchup is performed for valid transactionId and StoreId
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( from( testCatchupServer.getPort() ) );
        subject.copyStoreFiles( catchupAddressProvider, testCatchupServer.getStoreId(), storeFileStream, () -> defaultTerminationCondition );

        // then the catchup is successful
        Set<String> expectedFiles = new HashSet<>( Arrays.asList( fileA.getFilename(), fileB.getFilename(), indexFileA.getFilename() ) );
        assertEquals( expectedFiles, storeFileStream.filesystem.keySet() );
        assertEquals( fileContent( relative( fileA.getFilename() ) ), clientFileContents( storeFileStream, fileA.getFilename() ) );
        assertEquals( fileContent( relative( fileB.getFilename() ) ), clientFileContents( storeFileStream, fileB.getFilename() ) );
    }

    @Test
    public void failedFileCopyShouldRetry() throws StoreCopyFailedException
    {
        // given a file will fail twice before succeeding
        fileB.setRemainingFailed( 2 );

        // and local client has a store
        InMemoryFileSystemStream clientStoreFileStream = new InMemoryFileSystemStream();

        // when catchup is performed for valid transactionId and StoreId
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( from( testCatchupServer.getPort() ) );
        subject.copyStoreFiles( catchupAddressProvider, testCatchupServer.getStoreId(), clientStoreFileStream, () -> defaultTerminationCondition );

        // then the catchup is successful
        Set<String> expectedFiles = new HashSet<>( Arrays.asList( fileA.getFilename(), fileB.getFilename(), indexFileA.getFilename() ) );
        assertEquals( expectedFiles, clientStoreFileStream.filesystem.keySet() );

        // and
        assertEquals( fileContent( relative( fileA.getFilename() ) ), clientFileContents( clientStoreFileStream, fileA.getFilename() ) );
        assertEquals( fileContent( relative( fileB.getFilename() ) ), clientFileContents( clientStoreFileStream, fileB.getFilename() ) );

        // and verify server had exactly 2 failed calls before having a 3rd succeeding request
        assertEquals( 3, testCatchupServer.getRequestCount( fileB.getFilename() ) );

        // and verify server had exactly 1 call for all other files
        assertEquals( 1, testCatchupServer.getRequestCount( fileA.getFilename() ) );
    }

    @Test
    public void reconnectingWorks() throws StoreCopyFailedException
    {
        // given local client has a store
        InMemoryFileSystemStream storeFileStream = new InMemoryFileSystemStream();

        // and file B is broken once (after retry it works)
        fileB.setRemainingNoResponse( 1 );

        // when catchup is performed for valid transactionId and StoreId
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( from( testCatchupServer.getPort() ) );
        subject.copyStoreFiles( catchupAddressProvider, testCatchupServer.getStoreId(), storeFileStream, () -> defaultTerminationCondition );

        // then the catchup is possible to complete
        assertEquals( fileContent( relative( fileA.getFilename() ) ), clientFileContents( storeFileStream, fileA.getFilename() ) );
        assertEquals( fileContent( relative( fileB.getFilename() ) ), clientFileContents( storeFileStream, fileB.getFilename() ) );

        // and verify file was requested more than once
        assertThat( testCatchupServer.getRequestCount( fileB.getFilename() ), greaterThan( 1 ) );
    }

    @Test
    public void shouldNotAppendToFileWhenRetryingWithNewFile() throws IOException, StoreCopyFailedException
    {
        // given
        String fileName = "foo";
        String pageCacheFileName = "bar";
        String unfinishedContent = "abcd";
        String finishedContent = "abcdefgh";
        Iterator<String> contents = Iterators.iterator( unfinishedContent, finishedContent );

        // and
        StrippedCatchupServer halfWayFailingServer = new StrippedCatchupServer()
        {
            @Override
            protected ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol protocol )
            {
                return new SimpleChannelInboundHandler<GetStoreFileRequest>()
                {
                    @Override
                    protected void channelRead0( ChannelHandlerContext ctx, GetStoreFileRequest msg ) throws IOException
                    {
                        // create the files and write the given content
                        File file = new File( fileName );
                        String thisConent = contents.next();
                        writeContents( fileSystemAbstraction, file, thisConent );
                        PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystemAbstraction );
                        PagedFile pagedFile =
                                pageCache.map( new File( pageCacheFileName ), pageCache.pageSize(), StandardOpenOption.CREATE, StandardOpenOption.WRITE );
                        try ( WritableByteChannel writableByteChannel = pagedFile.openWritableByteChannel() )
                        {
                            writableByteChannel.write( ByteBuffer.wrap( thisConent.getBytes() ) );
                        }

                        sendFile( ctx, file, pageCache );
                        sendFile( ctx, pagedFile.file(), pageCache );
                        StoreCopyFinishedResponse.Status status =
                                contents.hasNext() ? StoreCopyFinishedResponse.Status.E_UNKNOWN : StoreCopyFinishedResponse.Status.SUCCESS;
                        new StoreFileStreamingProtocol().end( ctx, status );
                        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                    }

                    private void sendFile( ChannelHandlerContext ctx, File file, PageCache pageCache )
                    {
                        ctx.write( ResponseMessageType.FILE );
                        ctx.write( new FileHeader( file.getName() ) );
                        ctx.writeAndFlush( new FileSender( new StoreResource( file, file.getName(), 16, pageCache, fileSystemAbstraction ) ) ).addListener(
                                future -> fileSystemAbstraction.deleteFile( file ) );
                    }
                };
            }

            @Override
            protected ChannelHandler getStoreListingRequestHandler( CatchupServerProtocol protocol )
            {
                return new SimpleChannelInboundHandler<PrepareStoreCopyRequest>()
                {
                    @Override
                    protected void channelRead0( ChannelHandlerContext ctx, PrepareStoreCopyRequest msg )
                    {
                        ctx.write( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE );
                        ctx.writeAndFlush( PrepareStoreCopyResponse.success( new File[]{new File( fileName )}, new Empty.EmptyPrimitiveLongSet(), 1 ) );
                        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                    }
                };
            }

            @Override
            protected ChannelHandler getIndexRequestHandler( CatchupServerProtocol protocol )
            {
                return new SimpleChannelInboundHandler<GetIndexFilesRequest>()
                {
                    @Override
                    protected void channelRead0( ChannelHandlerContext ctx, GetIndexFilesRequest msg )
                    {
                        throw new IllegalStateException( "There should not be any index requests" );
                    }
                };
            }
        };

        try
        {
            // when
            halfWayFailingServer.before();
            halfWayFailingServer.start();

            CatchupAddressProvider addressProvider = CatchupAddressProvider.fromSingleAddress( from( halfWayFailingServer.getPort() ) );
            StoreId storeId = halfWayFailingServer.getStoreId();
            File storeDir = testDirectory.makeGraphDbDir();
            PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystemAbstraction );
            StreamToDisk streamToDisk = new StreamToDisk( storeDir, fileSystemAbstraction, pageCache, new Monitors() );

            // and
            subject.copyStoreFiles( addressProvider, storeId, streamToDisk, () -> defaultTerminationCondition );

            // then
            assertEquals( fileContent( new File( storeDir, fileName ) ), finishedContent );

            // and
            PagedFile pagedFile = pageCache.map( new File( storeDir, pageCacheFileName ), pageCache.pageSize(), StandardOpenOption.READ );
            ByteBuffer buffer = ByteBuffer.wrap( new byte[finishedContent.length()] );
            try ( ReadableByteChannel readableByteChannel = pagedFile.openReadableByteChannel() )
            {
                readableByteChannel.read( buffer );
            }
            assertEquals( finishedContent, new String( buffer.array(), Charsets.UTF_8 ) );
        }
        finally
        {
            halfWayFailingServer.stop();
        }
    }

    private static AdvertisedSocketAddress from( int port )
    {
        return new AdvertisedSocketAddress( "localhost", port );
    }

    private File relative( String filename )
    {
        return testDirectory.file( filename );
    }

    private String fileContent( File file )
    {
        return fileContent( file, fileSystemAbstraction );
    }

    private static StringBuilder serverFileContentsStringBuilder( File file, FileSystemAbstraction fileSystemAbstraction )
    {
        try ( StoreChannel storeChannel = fileSystemAbstraction.open( file, OpenMode.READ ) )
        {
            final int MAX_BUFFER_SIZE = 100;
            ByteBuffer byteBuffer = ByteBuffer.wrap( new byte[MAX_BUFFER_SIZE] );
            StringBuilder stringBuilder = new StringBuilder();
            Predicate<Integer> inRange = betweenZeroAndRange( MAX_BUFFER_SIZE );
            Supplier<Integer> readNext = unchecked( () -> storeChannel.read( byteBuffer ) );
            for ( int readBytes = readNext.get(); inRange.test( readBytes ); readBytes = readNext.get() )
            {
                for ( byte index = 0; index < readBytes; index++ )
                {
                    char actual = (char) byteBuffer.get( index );
                    stringBuilder.append( actual );
                }
            }
            return stringBuilder;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    static String fileContent( File file, FileSystemAbstraction fileSystemAbstraction )
    {
        return serverFileContentsStringBuilder( file, fileSystemAbstraction ).toString();
    }

    private static Supplier<Integer> unchecked( ThrowingSupplier<Integer,?> throwableSupplier )
    {
        return () ->
        {
            try
            {
                return throwableSupplier.get();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
        };
    }

    private static Predicate<Integer> betweenZeroAndRange( int range )
    {
        return bytes -> bytes > 0 && bytes <= range;
    }

    private String clientFileContents( InMemoryFileSystemStream storeFileStreams, String filename )
    {
        return storeFileStreams.filesystem.get( filename ).toString();
    }
}
